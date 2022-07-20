package org.kingdari.MiniBase;

import org.apache.log4j.Logger;
import org.kingdari.MiniBase.MStore.SeekIter;
import org.kingdari.MiniBase.Store.Compactor;
import org.kingdari.MiniBase.Store.Flusher;
import org.kingdari.MiniBase.Store.Iter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiskStore implements Closeable {

	private static final Logger LOG = Logger.getLogger(DiskStore.class);
	private static final String FILE_NAME_TMP_SUFFIX = ".tmp";
	private static final String FILE_NAME_ARCHIVE_SUFFIX = ".archive";
	private static final Pattern DATA_FILE_REGEX = Pattern.compile("data\\.([0-9]+)");

	public static class DefaultFlusher implements Flusher {
		private DiskStore diskStore;

		public DefaultFlusher(DiskStore diskStore) {
			this.diskStore = diskStore;
		}

		@Override
		public void flush(Iter<KeyValue> iter) throws IOException {
			String fileName = diskStore.getNextDiskFileName();
			String fileTempName = fileName + FILE_NAME_TMP_SUFFIX;
			try {
				try (DiskFileWriter writer = new DiskFileWriter(fileTempName)) {
					while(iter.hasNext()) {
						writer.append(iter.next());
					}
					writer.appendIndex();
					writer.appendTrailer();
				}
				File f = new File(fileTempName);
				if (!f.renameTo(new File(fileName))) {
					throw new IOException(
							String.format("Rename %s to %s failed", fileTempName, fileName));
				}
				diskStore.addDiskFile(fileName);
			} finally {
				File f = new File(fileTempName);
				if (f.exists()) {
					f.delete();
				}
			}
		}
	}

	public static class DefaultCompactor extends Compactor {
		private DiskStore diskStore;
		private volatile boolean running;
		private final AtomicBoolean compacting;

		public DefaultCompactor(DiskStore diskStore) {
			this.diskStore = diskStore;
			this.running = true;
			this.compacting = new AtomicBoolean(false);
			this.setDaemon(true);
		}

		private void performCompact(List<DiskFile> filesToCompact) throws IOException {
			if (!compacting.compareAndSet(false, true)) {
				LOG.info("Now is compacting...");
				return;
			}
			String fileName = diskStore.getNextDiskFileName();
			String fileTempName = fileName + FILE_NAME_TMP_SUFFIX;
			try {
				try (DiskFileWriter dfw = new DiskFileWriter(fileTempName)) {
					try (Iter<KeyValue> iter = diskStore.createIterator(filesToCompact)) {
						while (iter.hasNext()) {
							dfw.append(iter.next());
						}
					}
					dfw.appendIndex();
					dfw.appendTrailer();
				}
				File f = new File(fileTempName);
				if (!f.renameTo(new File(fileName))) {
					throw new IOException("Rename fail: " + fileTempName);
				}

				diskStore.compactDown(filesToCompact, new DiskFile(fileName, diskStore.cache).open());
			} finally {
				File f = new File(fileTempName);
				if (f.exists()) {
					f.delete();
				}
				if (!compacting.compareAndSet(true, false)) {
					LOG.error("Unexpected CAS failure");
				}
			}
		}

		@Override
		public void compact() throws IOException {
			performCompact(diskStore.getDiskFilesSnapshot());
		}

		@Override
		public void run() {
			while(running) {
				try {
					boolean isCompacted = false;
					if (diskStore.getDiskFilesSnapshot().size() > diskStore.getMaxDiskFiles()) {
						performCompact(diskStore.getDiskFilesSnapshot());
						isCompacted = true;
					}

					diskStore.tryClearCompactedDiskFiles();

					if (!isCompacted) {
						Thread.sleep(1000);
					}
				} catch (IOException e) {
					e.printStackTrace();
					LOG.error("Major Compaction failed", e);
				} catch (InterruptedException e) {
					LOG.error("interrupt");
					break;
				}
			}
		}

		@Override
		public void stopRunning() {
			running = false;
			LOG.info("Compactor try to stop running...");
			try {
				diskStore.tryClearCompactedDiskFiles();
				join();
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
			LOG.info("Compactor exited.");
		}
	}

	public static class MultiIter implements SeekIter<KeyValue> {

		private static class IterNode {
			KeyValue kv;
			SeekIter<KeyValue> iter;

			public IterNode(KeyValue kv, SeekIter<KeyValue> iter) {
				this.kv = kv;
				this.iter = iter;
			}
		}

		private SeekIter<KeyValue> iters[];
		private PriorityQueue<IterNode> queue;
		private KeyValueFilter filter;

		public MultiIter(SeekIter<KeyValue>[] iters) throws IOException {
			assert iters != null;
			this.iters = iters;
			this.queue = new PriorityQueue<>(Comparator.comparing(node -> node.kv));
			for (SeekIter<KeyValue> iter : iters) {
				if (iter != null && iter.hasNext()) {
					queue.add(new IterNode(iter.next(), iter));
				}
			}
		}

		@SuppressWarnings("unchecked")
		public MultiIter(List<SeekIter<KeyValue>> iters) throws IOException {
			this(iters.toArray(new SeekIter[0]));
		}

		@Override
		public void seekTo(KeyValue kv) throws IOException {
			queue.clear();
			for (SeekIter<KeyValue> iter : iters) {
				if (iter == null) {
					continue;
				}
				iter.seekTo(kv);
				if (iter.hasNext()) {
					queue.add(new IterNode(iter.next(), iter));
				}
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			return queue.size() > 0;
		}

		@Override
		public KeyValue next() throws IOException {
			while (!queue.isEmpty()) {
				IterNode smallest = queue.poll();
				if (smallest.kv != null && smallest.iter != null) {
					if (smallest.iter.hasNext()) {
						queue.add(new IterNode(smallest.iter.next(), smallest.iter));
					}
					return smallest.kv;
				}
			}
			return null;
		}

		@Override
		public void close() {
			for (SeekIter<KeyValue> iter : iters) {
				iter.close();
			}
		}
	}

	private String dataDir;

	// Guard diskFiles;
	private ReentrantReadWriteLock diskFilesLock;
	private List<DiskFile> diskFiles;
	// Guard compactedDiskFiles;
	private ReentrantReadWriteLock compactedDiskFilesLock;
	// Already compact data to new file. But maybe some iterator is scanning old disk files.
	private List<DiskFile> compactedDiskFiles;

	private BlockCache cache;

	private final int maxDiskFiles;
	private volatile AtomicLong maxFileId;

	private File[] listDiskFiles() {
		final File[] files = new File(dataDir).listFiles(
				filename -> DATA_FILE_REGEX.matcher(filename.getName()).matches());
		return files == null ? new File[]{} : files;
	}

	public DiskStore(String dataDir, int maxDiskFiles, long maxBlockCacheSize) {
		this.dataDir = dataDir;
		this.maxDiskFiles = maxDiskFiles;
		this.diskFiles = new ArrayList<>();
		this.compactedDiskFiles = new ArrayList<>();
		this.diskFilesLock = new ReentrantReadWriteLock();
		this.compactedDiskFilesLock = new ReentrantReadWriteLock();
		this.cache = new BlockCache(maxBlockCacheSize);
	}

	public void open() throws IOException {
		File[] files = listDiskFiles();
		long maxFileId = 0L;
		for (File f : files) {
			Matcher matcher = DATA_FILE_REGEX.matcher(f.getName());
			if (matcher.matches()) {
				maxFileId = Math.max(maxFileId, Long.parseLong(matcher.group(1)));
			} else {
				assert false;
			}
		}
		this.maxFileId = new AtomicLong(maxFileId);

		diskFilesLock.writeLock().lock();
		try {
			for (File f : files) {
				DiskFile df = new DiskFile(f.getAbsolutePath(), cache).open();
				diskFiles.add(df);
			}
		} finally {
			diskFilesLock.writeLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		IOException closedException = null;
		LOG.info("DiskStore try to close disk files...");
		diskFilesLock.readLock().lock();
		try {
			for (DiskFile df : diskFiles) {
				try {
					df.close();
				} catch (IOException e) {
					closedException = e;
				}
			}
		} finally {
			diskFilesLock.readLock().unlock();
		}
		compactedDiskFilesLock.readLock().lock();
		try {
			for (DiskFile df : compactedDiskFiles) {
				try {
					df.close();
				} catch (IOException e) {
					closedException = e;
				}
			}
		} finally {
			compactedDiskFilesLock.readLock().unlock();
		}
		if (closedException != null) {
			throw closedException;
		}
		LOG.info("DiskStore exited.");
	}

	public long nextDiskFileId() {
		return maxFileId.incrementAndGet();
	}

	public String getNextDiskFileName() {
		return new File(dataDir, String.format("data.%020d", nextDiskFileId())).getPath();
	}

	public void addDiskFile(DiskFile diskFile) {
		diskFilesLock.writeLock().lock();
		try {
			diskFiles.add(diskFile);
		} finally {
			diskFilesLock.writeLock().unlock();
		}
	}

	public void addDiskFile(String fileName) throws IOException {
		addDiskFile(new DiskFile(fileName, cache).open());
	}

	public List<DiskFile> getDiskFilesSnapshot() {
		diskFilesLock.readLock().lock();
		try {
			return new ArrayList<>(diskFiles);
		} finally {
			diskFilesLock.readLock().unlock();
		}
	}

	public void compactDown(Collection<DiskFile> files, DiskFile newFile) {
		diskFilesLock.writeLock().lock();
		compactedDiskFilesLock.writeLock().lock();
		try {
			int oldSize = diskFiles.size();
			diskFiles.removeAll(files);
			diskFiles.add(newFile);
			compactedDiskFiles.addAll(files);
			LOG.info(String.format("Compact down. DiskFiles size: %d -> %d",
							oldSize, diskFiles.size()));
		} finally {
			compactedDiskFilesLock.writeLock().unlock();
			diskFilesLock.writeLock().unlock();
		}
	}

	public void tryClearCompactedDiskFiles() throws IOException {
		compactedDiskFilesLock.writeLock().lock();
		try {
			List<DiskFile> unusedDiskFiles = new ArrayList<>();
			for (DiskFile df : compactedDiskFiles) {
				if (df.getRefCount() != 0) {
					continue;
				}
				df.close();
				unusedDiskFiles.add(df);
				File file = new File(df.getFileName());
				File archiveFile = new File(df.getFileName() + FILE_NAME_ARCHIVE_SUFFIX);
				if (!file.renameTo(archiveFile)) {
					throw new IOException("Rename fail: " + archiveFile.getName());
				}
			}
			compactedDiskFiles.removeAll(unusedDiskFiles);
		} finally {
			compactedDiskFilesLock.writeLock().unlock();
		}
	}

	public int getMaxDiskFiles() {
		return maxDiskFiles;
	}

	public SeekIter<KeyValue> createIterator(List<DiskFile> diskFiles) throws IOException {
		return createIterator(diskFiles, new KeyValueFilter());
	}

	public SeekIter<KeyValue> createIterator() throws IOException {
		return createIterator(getDiskFilesSnapshot(), new KeyValueFilter());
	}

	public SeekIter<KeyValue> createIterator(KeyValueFilter filter) throws IOException {
		return createIterator(getDiskFilesSnapshot(), filter);
	}

	public SeekIter<KeyValue> createIterator(List<DiskFile> diskFiles, KeyValueFilter filter) throws IOException {
		List<SeekIter<KeyValue>> iters = new ArrayList<>(diskFiles.size());
		diskFiles.forEach(df -> iters.add(df.iterator(filter)));
		return new MultiIter(iters);
	}
}
