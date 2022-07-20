package org.kingdari.MiniBase;

import org.apache.log4j.Logger;
import org.kingdari.MiniBase.DiskStore.MultiIter;
import org.kingdari.MiniBase.MStore.SeekIter;
import org.kingdari.MiniBase.Store.Flusher;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemStore implements Closeable {

	private static final Logger LOG = Logger.getLogger(MemStore.class);

	public static class IteratorWrapper implements SeekIter<KeyValue> {

		private SortedMap<KeyValue, KeyValue> sortedMap;
		private Iterator<KeyValue> it;
		private KeyValueFilter filter;
		private KeyValue nextKv;

		public IteratorWrapper(SortedMap<KeyValue, KeyValue> sortedMap, KeyValueFilter filter) {
			this.sortedMap = sortedMap;
			this.it = sortedMap.values().iterator();
			this.filter = filter;
		}

		@Override
		public void seekTo(KeyValue kv) throws IOException {
			it = sortedMap.tailMap(kv).values().iterator();
			nextKv = null;
		}

		@Override
		public boolean hasNext() throws IOException {
			if (nextKv == null) {
				while (it.hasNext()) {
					KeyValue kv = it.next();
					if (filter.isVisible(kv)) {
						nextKv = kv;
						break;
					}
				}
			}
			return nextKv != null;
		}

		@Override
		public KeyValue next() throws IOException {
			if (nextKv == null && !hasNext()) {
				return null;
			}
			KeyValue kv = nextKv;
			nextKv = null;
			return kv;
		}

		@Override
		public void close() {

		}
	}

	private static class MemStoreIter implements SeekIter<KeyValue> {
		private MultiIter iter;
		private MemStore memStore;
		private KeyValueFilter filter;

		public MemStoreIter(MemStore memStore, KeyValueFilter filter) throws IOException {
			this.memStore = memStore;
			this.filter = filter;
			ConcurrentSkipListMap<KeyValue, KeyValue>[] memStoreMaps = memStore.getMemStoreMaps();
			this.iter = new MultiIter(new SeekIter[]{
					new IteratorWrapper(memStoreMaps[0], filter),
					new IteratorWrapper(memStoreMaps[1], filter)});
		}

		@Override
		public void seekTo(KeyValue kv) throws IOException {
			iter.seekTo(kv);
		}

		@Override
		public boolean hasNext() throws IOException {
			return iter.hasNext();
		}

		@Override
		public KeyValue next() throws IOException {
			return iter.next();
		}

		@Override
		public void close() {
			iter.close();
		}
	}

	private class FlusherTask implements Runnable {
		@Override
		public void run() {
			boolean success = false;
			for (int i = 0; i < conf.getFlushMaxRetryTimes(); i++) {
				try {
					flusher.flush(new IteratorWrapper(kvImmutableMap, KeyValueFilter.createEmptyFilter()));
					success = true;
					break;
				} catch (IOException e) {
					LOG.warn(String.format("Failed to flush memStore. Retry %d of %d",
							i + 1, conf.getFlushMaxRetryTimes()), e);
				}
			}

			if (success) {
				kvImmutableMap = new ConcurrentSkipListMap<>();
				if (!isImmutableMapFlushing.compareAndSet(true, false)) {
					LOG.error("Unexpected CAS Fail");
				}
			}
		}
	}

	private final AtomicLong dataSize = new AtomicLong();

	private volatile ConcurrentSkipListMap<KeyValue, KeyValue> kvMap;
	private volatile ConcurrentSkipListMap<KeyValue, KeyValue> kvImmutableMap;

	/**
	 * guard dataSize(only rw, rr guarded by CAS), kvMap, kvSnapshot
	 * rlock: writing to memory
	 * wlock: flushing memory
	 */
	private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();
	private final AtomicBoolean isImmutableMapFlushing = new AtomicBoolean();
	private ExecutorService pool;
	private Config conf;
	private Flusher flusher;

	public MemStore(Config conf, Flusher flusher, ExecutorService pool) {
		this.conf = conf;
		this.flusher = flusher;
		this.pool = pool;

		dataSize.set(0);

		this.kvMap = new ConcurrentSkipListMap<>();
		this.kvImmutableMap = new ConcurrentSkipListMap<>();
	}

	public void add(KeyValue kv) throws IOException {
		flushIfNeeded(true);
		updateLock.readLock().lock();
		try {
			KeyValue prevKv = kvMap.put(kv, kv);
			if (prevKv == null) {
				dataSize.addAndGet(kv.getSerializedSize());
			} else {
				dataSize.addAndGet(kv.getSerializedSize() - prevKv.getSerializedSize());
			}
		} finally {
			updateLock.readLock().unlock();
		}
		flushIfNeeded(false);
	}

	private void flushIfNeeded(boolean mustFlush) throws IOException {
		if (getDataSize() > conf.getMaxMemStoreSize()) {
			if (isFlushing() && mustFlush) {
				throw new IOException("MemStore is full, now is flushing, wait and retry");
			} else if (isImmutableMapFlushing.compareAndSet(false, true)) {
				// dataSize may less than config value due to concurrent issue.
				updateLock.writeLock().lock();
				try {
					// double check
					if (getDataSize() <= conf.getMaxMemStoreSize()) {
						if (isImmutableMapFlushing.compareAndSet(true, false)) {
							return;
						} else {
							throw new RuntimeException("Unexpected CAS Fail");
						}
					}
					kvImmutableMap = kvMap;
					kvMap = new ConcurrentSkipListMap<>();
					dataSize.set(0);
				} finally {
					updateLock.writeLock().unlock();
				}
				pool.submit(new FlusherTask());
			}
		}
	}

	public long getDataSize() {
		return dataSize.get();
	}

	public boolean isFlushing() {
		return isImmutableMapFlushing.get();
	}

	public ConcurrentSkipListMap<KeyValue, KeyValue>[] getMemStoreMaps() {
		updateLock.readLock().lock();
		try {
			return new ConcurrentSkipListMap[] {kvMap, kvImmutableMap};
		} finally {
			updateLock.readLock().unlock();
		}
	}

	public SeekIter<KeyValue> createIterator() throws IOException {
		return createIterator(new KeyValueFilter());
	}

	public SeekIter<KeyValue> createIterator(KeyValueFilter filter) throws IOException {
		return new MemStoreIter(this, filter);
	}

	@Override
	public void close() throws IOException {
		pool.shutdown();
		try {
			LOG.info("MemStore is waiting for flushing thread...");
			if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
				pool.shutdownNow();
				LOG.error("Timeout.");
			}
		} catch (InterruptedException e) {
			pool.shutdownNow();
			Thread.currentThread().interrupt();
		}
		LOG.info("MemStore exited.");
	}
}
