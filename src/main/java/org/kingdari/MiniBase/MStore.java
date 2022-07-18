package org.kingdari.MiniBase;

import org.apache.log4j.Logger;
import org.kingdari.MiniBase.DiskStore.DefaultCompactor;
import org.kingdari.MiniBase.DiskStore.DefaultFlusher;
import org.kingdari.MiniBase.DiskStore.MultiIter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MStore implements Store {
	private static final Logger LOG = Logger.getLogger(MStore.class);

	/**
	 * Not yet supported scan with version.
	 */
	public static class ScanIter implements Iter<KeyValue> {

		private KeyValue stopKv;
		private Iter<KeyValue> iter;
		// lastKv: kv which has the largest seqId with same Key.
		private KeyValue lastKv;
		private KeyValue pendingKv;

		public ScanIter(KeyValue stopKv, SeekIter<KeyValue> iter) {
			this.stopKv = stopKv;
			this.iter = iter;
			this.lastKv = null;
			this.pendingKv = null;
		}

		private boolean shouldStop(KeyValue kv) {
			return stopKv != null &&
					ByteUtils.compare(stopKv.getKey(), kv.getKey()) <= 0;
		}

		private void switchToNewKey() throws IOException {
			if (lastKv != null && shouldStop(lastKv)) {
				return;
			}
			KeyValue curKv;
			while(iter.hasNext()) {
				curKv = iter.next();
				if (shouldStop(curKv)) {
					return;
				}
				if (curKv.getOp() == KeyValue.Op.Put) {
					if (lastKv == null) {
						lastKv = pendingKv = curKv;
						return;
					}
					// compare Key.
					int ret = ByteUtils.compare(lastKv.getKey(), curKv.getKey());
					if (ret < 0) {
						lastKv = pendingKv = curKv;
						return;
					} else if (ret > 0) {
						LOG.error("Illegal state: lastKv > curKv");
					}
				} else if (curKv.getOp() == KeyValue.Op.Delete) {
					if (lastKv == null ||
							ByteUtils.compare(lastKv.getKey(), curKv.getKey()) < 0) {
						lastKv = curKv;
					} else if (ByteUtils.compare(lastKv.getKey(), curKv.getKey()) > 0) {
						LOG.error("Illegal state: lastKv > curKv");
					}
				} else {
					throw new RuntimeException("Unknown op code: " + curKv.getOp());
				}
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			if (pendingKv == null) {
				switchToNewKey();
			}
			return pendingKv != null;
		}

		@Override
		public KeyValue next() throws IOException {
			if (pendingKv == null) {
				switchToNewKey();
			}
			lastKv = pendingKv;
			pendingKv = null;
			return lastKv;
		}

		@Override
		public void close() {
			iter.close();
		}
	}

	public static MStore create() {
		return new MStore(Config.getDefault());
	}

	public static MStore create(Config conf) {
		return new MStore(conf);
	}

	private ExecutorService pool;
	private MemStore memStore;
	private DiskStore diskStore;
	private Compactor compactor;
	private MLog log;
	private AtomicLong globalSeqId;
	private LogConsumer logConsumer;

	private Config conf;

	private MStore(Config conf) {
		this.conf = conf;
	}

	public Store open() throws IOException {
		assert conf != null;

		initFile();

		this.pool = Executors.newFixedThreadPool(conf.getMaxThreadPoolSize());
		this.diskStore = new DiskStore(conf.getFullDataDir(), conf.getMaxDiskFiles());
		this.diskStore.open();
		this.globalSeqId = new AtomicLong(0);

		this.log = new BaseLog(conf);
		this.memStore = new MemStore(conf, new DefaultFlusher(diskStore), pool);
		this.logConsumer = new LogConsumer(conf, (BaseLog) log, memStore, this);
		this.logConsumer.start();

		this.compactor = new DefaultCompactor(diskStore);
		this.compactor.start();
		LOG.info("MiniBase open successfully");
		return this;
	}

	private KeyValue scanGet(byte[] key) throws IOException {
		final Iter<KeyValue> iter = scan(key, ByteUtils.EMPTY_BYTES);
		if (iter.hasNext()) {
			final KeyValue kv = iter.next();
			if (ByteUtils.compare(key, kv.getKey()) == 0) {
				return kv;
			}
		}
		return null;
	}

	private KeyValue bfGet(byte[] key) throws IOException {
		final Iter<KeyValue> iter =
				scan(key, ByteUtils.EMPTY_BYTES, new KeyValueFilter().setKey(key));
		if (iter.hasNext()) {
			final KeyValue kv = iter.next();
			if (ByteUtils.compare(key, kv.getKey()) == 0) {
				return kv;
			}
		}
		return null;
	}

	public long updateGlobalVersion(long seq) {
		long now;
		do {
			now = globalSeqId.get();
			if (now >= seq) {
				return now;
			}
		} while (globalSeqId.compareAndSet(now, seq));
		return seq;
	}

	private void initFile() {
		File dataDir = new File(conf.getFullDataDir());
		dataDir.mkdirs();
		File logDir = new File(conf.getFullLogDir());
		logDir.mkdirs();
	}

	@Override
	public void put(byte[] key, byte[] value) throws IOException {
		// memStore.add(KeyValue.createPut(key, value, sequenceId.incrementAndGet()));
		log.put(key, value);
	}

	@Override
	public KeyValue get(byte[] key) throws IOException {
		return bfGet(key);
	}

	@Override
	public void delete(byte[] key) throws IOException {
		// memStore.add(KeyValue.createDelete(key, sequenceId.incrementAndGet()));
		log.delete(key);
	}

	@Override
	public Iter<KeyValue> scan(byte[] start, byte[] end, KeyValueFilter filter) throws IOException {
		List<SeekIter<KeyValue>> iters = new ArrayList<>();
		iters.add(memStore.createIterator());
		// memStore first, diskStore second.
		// maybe here immutableMap flush to disk.
		// In this case, read duplicate data instead of losing immutableMap data.
		iters.add(diskStore.createIterator(filter));
		MultiIter iter = new MultiIter(iters);

		// EMPTY BYTE means infinity.
		if (ByteUtils.compare(start, ByteUtils.EMPTY_BYTES) != 0) {
			iter.seekTo(KeyValue.createDelete(start, globalSeqId.get()));
		}

		KeyValue stopKv = null;
		if (ByteUtils.compare(end, ByteUtils.EMPTY_BYTES) != 0) {
			stopKv = KeyValue.createDelete(end, Long.MAX_VALUE);
		}
		return new ScanIter(stopKv, iter);
	}

	@Override
	public void close() throws IOException {
		logConsumer.close();
		compactor.stopRunning(); // waiting compact finish.
		memStore.close();
		diskStore.close();
	}

	interface SeekIter<KeyValue> extends Iter<KeyValue> {
		/**
		 * Seek to the smallest kv which is greater than or equals to param.
		 * @param kv
		 * @throws IOException
		 */
		void seekTo(KeyValue kv) throws IOException;
	}
}
