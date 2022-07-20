package org.kingdari.MiniBase;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.kingdari.MiniBase.BlockReader.DataBlockIterator;
import org.kingdari.MiniBase.MStore.SeekIter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

public class DiskFile implements Closeable {
	private static final Logger LOG = LogManager.getLogger(DiskFile.class);

	public static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;
	public static final int BLOOM_FILTER_HASH_COUNT = 3;
	public static final int BLOOM_FILTER_BITS_PER_KEY = 10;

	// fileSize(8) + blockCount(4) + blockIndexOffset(8) +
	// blockIndexSize(8) + DISK_FILE_MAGIC(8)
	public static final int TRAILER_SIZE = 8 + 4 + 8 + 8 + 8;
	public static final long DISK_FILE_MAGIC = 0x19980825_DEAD10CCL;

	private static class DiskFileIterator implements SeekIter<KeyValue> {

		private DiskFile df;
		private int currentKvIndex = 0;
		private BlockReader currentReader;
		private KeyValueFilter filter;
		private SortedSet<BlockMeta> filteredMeta;
		private Iterator<BlockMeta> blockMetaIter;
		private DataBlockIterator currentDataIter;

		public DiskFileIterator(DiskFile df, KeyValueFilter filter) {
			this.currentReader = null;
			this.currentDataIter = null;
			this.df = df;
			this.filter = filter;
			this.filteredMeta = filter.getFilteredMeta(df);
			this.blockMetaIter = filteredMeta.iterator();
		}

		private boolean nextBlockReader() throws IOException {
			while (blockMetaIter.hasNext()) {
				currentReader = df.loadReader(blockMetaIter.next());
				currentDataIter = currentReader.iterator(filter);
				if (currentDataIter.hasNext()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public void seekTo(KeyValue targetKv) throws IOException {
			blockMetaIter = filteredMeta.tailSet(BlockMeta.createSeekDummy(targetKv)).iterator();
			currentReader = null;
			currentDataIter = null;
			while (blockMetaIter.hasNext()) {
				currentReader = df.loadReader(blockMetaIter.next());
				currentDataIter = currentReader.iterator(filter);
				currentDataIter.seekTo(targetKv);
				if (currentDataIter.hasNext()) {
					break;
				}
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			if (currentReader != null && currentDataIter.hasNext()) {
				return true;
			} else {
				return nextBlockReader();
			}
		}

		@Override
		public KeyValue next() throws IOException {
			return currentDataIter.next();
		}

		@Override
		public void close() {
			df.refCount.decrementAndGet();
		}
	}

	private SortedSet<BlockMeta> blockMetaSet = new TreeSet<>();
	private RandomAccessFile in;
	private BlockCache cache;

	private final String fileName;
	private long fileSize;
	private int blockCount;
	private long blockIndexOffset;
	private long blockIndexSize;
	private final AtomicInteger refCount = new AtomicInteger(0);

	public DiskFile(String fileName) {
		this(fileName, null);
	}

	public DiskFile(String fileName, BlockCache cache) {
		this.fileName = fileName;
		this.cache = cache;
	}

	private BlockReader loadReader(BlockMeta meta) throws IOException {
		if (cache == null) {
			return createReader(meta);
		} else {
			return cache.get(new BlockId(this, meta));
		}
	}

	public BlockReader createReader(BlockMeta meta) throws IOException {
		byte[] buffer = new byte[(int) meta.getBlockSize()];
		// guard in.
		synchronized (this) {
			in.seek(meta.getBlockOffset());
			int len = in.read(buffer);
			assert len == buffer.length;
		}
		return BlockReader.parseFrom(buffer, 0, buffer.length);
	}

	public SortedSet<BlockMeta> getBlockMetaSet() {
		return blockMetaSet;
	}

	public int getRefCount() {
		return refCount.get();
	}

	public String getFileName() {
		return fileName;
	}

	public DiskFile open() throws IOException {
		File f = new File(fileName);
		this.in = new RandomAccessFile(f, "r");

		this.fileSize = f.length();
		assert fileSize > TRAILER_SIZE;
		in.seek(fileSize - TRAILER_SIZE);

		byte[] buffer = new byte[8];
		int len = in.read(buffer);
		assert len == buffer.length;
		assert this.fileSize == ByteUtils.toLong(buffer);

		buffer = new byte[4];
		len = in.read(buffer);
		assert len == buffer.length;
		this.blockCount = ByteUtils.toInt(buffer);

		buffer = new byte[8];
		len = in.read(buffer);
		assert len == buffer.length;
		this.blockIndexOffset = ByteUtils.toLong(buffer);

		buffer = new byte[8];
		len = in.read(buffer);
		assert len == buffer.length;
		this.blockIndexSize = ByteUtils.toLong(buffer);

		buffer = new byte[8];
		len = in.read(buffer);
		assert len == buffer.length;
		assert DISK_FILE_MAGIC == ByteUtils.toLong(buffer);

		buffer = new byte[(int) blockIndexSize];
		in.seek(blockIndexOffset);
		len = in.read(buffer);
		assert len == buffer.length;

		int offset = 0;

		do {
			BlockMeta meta = BlockMeta.parseFrom(buffer, offset);
			offset += meta.getSerializeSize();
			blockMetaSet.add(meta);
		} while (offset < buffer.length);
		assert blockMetaSet.size() == blockCount;
		assert offset == buffer.length;

		return this;
	}

	@Override
	public void close() throws IOException {
		int ref = refCount.get();
		if (ref != 0) {
			throw new IOException("Cannot close. RefCount: " + ref);
		}
		if (in != null) {
			in.close();
		}
	}

	public SeekIter<KeyValue> iterator() {
		return iterator(new KeyValueFilter());
	}

	public SeekIter<KeyValue> iterator(KeyValueFilter filter) {
		refCount.incrementAndGet();
		return new DiskFileIterator(this, filter);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		DiskFile diskFile = (DiskFile) o;
		return Objects.equals(fileName, diskFile.fileName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(fileName);
	}
}
