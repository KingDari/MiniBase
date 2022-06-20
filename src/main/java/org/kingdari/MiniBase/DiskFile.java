package org.kingdari.MiniBase;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.kingdari.MiniBase.MStore.SeekIter;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class DiskFile implements Closeable {
	private static final Logger LOG = LogManager.getLogger(DiskFile.class);

	public static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;
	public static final int BLOOM_FILTER_HASH_COUNT = 3;
	public static final int BLOOM_FILTER_BITS_PER_KEY = 10;

	// fileSize(8) + blockCount(4) + blockIndexOffset(8) +
	// blockIndexSize(8) + DISK_FILE_MAGIC(8)
	public static final int TRAILER_SIZE = 8 + 4 + 8 + 8 + 8;
	public static final long DISK_FILE_MAGIC = 0x19980825_DEAD10CCL;

	public static class BlockWriter {
		public static final int KV_SIZE_LEN = 4;
		public static final int CHECKSUM_LEN = 4;

		private int totalSize;
		private int kvCount;
		private List<KeyValue> kvBuf;
		private BloomFilter bloomFilter;
		private Checksum crc32;
		private KeyValue lastKv;

		public BlockWriter() {
			this.totalSize = 0;
			this.kvCount = 0;
			this.kvBuf = new ArrayList<>();
			this.bloomFilter =
					new BloomFilter(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_BITS_PER_KEY);
			this.crc32 = new CRC32();
		}

		public void append(KeyValue kv) throws IOException {
			kvBuf.add(kv);
			lastKv = kv;

			byte[] buf = kv.toBytes();
			crc32.update(buf, 0, buf.length);

			totalSize += kv.getSerializedSize();
			kvCount++;
		}

		public int getKvCount() {
			return kvCount;
		}

		public int size() {
			return KV_SIZE_LEN + totalSize + CHECKSUM_LEN;
		}

		public KeyValue getLastKv() {
			return lastKv;
		}

		private int getCheckSum() {
			return (int) crc32.getValue();
		}

		public byte[] getBloomFilter() {
			byte[][] bytes = new byte[kvBuf.size()][];
			for (int i = 0; i < kvBuf.size(); i++) {
				bytes[i] = kvBuf.get(i).getKey();
			}
			return bloomFilter.generate(bytes);
		}

		public byte[] serialize() {
			byte[] buffer = new byte[size()];
			int pos = 0;

			byte[] kvSize = ByteUtils.toBytes(kvBuf.size());
			System.arraycopy(kvSize, 0, buffer, pos, kvSize.length);
			pos += kvSize.length;

			for (KeyValue keyValue : kvBuf) {
				byte[] kv = keyValue.toBytes();
				System.arraycopy(kv, 0, buffer, pos, kv.length);
				pos += kv.length;
			}

			byte[] checksum = ByteUtils.toBytes(getCheckSum());
			System.arraycopy(checksum, 0, buffer, pos, checksum.length);
			pos += checksum.length;

			assert pos == size();
			return buffer;
		}
	}

	public static class BlockReader {
		private List<KeyValue> kvBuf;

		public BlockReader(List<KeyValue> kvBuf) {
			this.kvBuf = kvBuf;
		}

		public static BlockReader parseFrom(byte[] buffer, int offset, int size) throws IOException {
			int pos = offset;
			List<KeyValue> kvBuf = new ArrayList<>();
			Checksum crc32 = new CRC32();

			int kvSize = ByteUtils.toInt(ByteUtils.slice(buffer, pos, BlockWriter.KV_SIZE_LEN));
			pos += BlockWriter.KV_SIZE_LEN;

			for (int i = 0; i < kvSize; i++) {
				KeyValue kv = KeyValue.parseFrom(buffer, pos);
				kvBuf.add(kv);
				crc32.update(buffer, pos, kv.getSerializedSize());
				pos += kv.getSerializedSize();
			}

			int checksum = ByteUtils.toInt(ByteUtils.slice(buffer, pos, BlockWriter.CHECKSUM_LEN));
			pos += BlockWriter.CHECKSUM_LEN;

			assert checksum == (int) crc32.getValue();
			assert pos == size : "pos: " + pos + ", size: " + size;
			return new BlockReader(kvBuf);
		}

		public List<KeyValue> getKeyValues() {
			return kvBuf;
		}
	}

	public static class BlockMeta implements Comparable<BlockMeta> {

		private static final int OFFSET_SIZE = 8;
		private static final int SIZE_SIZE = 8;
		private static final int BF_LEN_SIZE = 4;

		private static BlockMeta createSeekDummy(KeyValue lastKv) {
			return new BlockMeta(lastKv, 0L, 0L, ByteUtils.EMPTY_BYTES);
		}

		private KeyValue lastKv;
		private long blockOffset;
		private long blockSize;
		private byte[] bfBytes;

		public BlockMeta(KeyValue lastKv, long offset, long size, byte[] bfBytes) {
			this.lastKv = lastKv;
			this.blockOffset = offset;
			this.blockSize = size;
			this.bfBytes = bfBytes;
		}

		public KeyValue getLastKv() {
			return lastKv;
		}

		public long getBlockOffset() {
			return blockOffset;
		}

		public long getBlockSize() {
			return blockSize;
		}

		public byte[] getBFBytes() {
			return bfBytes;
		}

		public int getSerializeSize() {
			return lastKv.getSerializedSize() + OFFSET_SIZE + SIZE_SIZE + BF_LEN_SIZE + bfBytes.length;
		}

		public byte[] toBytes() throws IOException {
			byte[] bytes = new byte[getSerializeSize()];
			int pos = 0;

			byte[] kvBytes = lastKv.toBytes();
			System.arraycopy(kvBytes, 0, bytes, pos, kvBytes.length);
			pos += kvBytes.length;

			byte[] offsetBytes = ByteUtils.toBytes(blockOffset);
			System.arraycopy(offsetBytes, 0, bytes, pos, offsetBytes.length);
			pos += offsetBytes.length;

			byte[] sizeBytes = ByteUtils.toBytes(blockSize);
			System.arraycopy(sizeBytes, 0, bytes, pos, sizeBytes.length);
			pos += sizeBytes.length;

			byte[] bfLenBytes = ByteUtils.toBytes(bfBytes.length);
			System.arraycopy(bfLenBytes, 0, bytes, pos, bfLenBytes.length);
			pos += bfLenBytes.length;

			System.arraycopy(bfBytes, 0, bytes, pos, bfBytes.length);
			pos += bfBytes.length;

			assert pos == bytes.length;
			return bytes;
		}

		public static BlockMeta parseFrom(byte[] buf, int offset) throws IOException {
			int pos = offset;
			KeyValue lastKv = KeyValue.parseFrom(buf, pos);
			pos += lastKv.getSerializedSize();
			long blockOffset = ByteUtils.toLong(ByteUtils.slice(buf, pos, OFFSET_SIZE));
			pos += OFFSET_SIZE;
			long blockSize = ByteUtils.toLong(ByteUtils.slice(buf, pos, SIZE_SIZE));
			pos += SIZE_SIZE;
			int bloomFilterSize = ByteUtils.toInt(ByteUtils.slice(buf, pos, BF_LEN_SIZE));
			pos += BF_LEN_SIZE;
			byte[] bloomFilter = ByteUtils.slice(buf, pos, bloomFilterSize);
			pos += bloomFilterSize;

			assert pos <= buf.length;
			return new BlockMeta(lastKv, blockOffset, blockSize, bloomFilter);
		}

		@Override
		public int compareTo(BlockMeta o) {
			return this.lastKv.compareTo(o.lastKv);
		}
	}

	public static class BlockIndexWriter {

		private List<BlockMeta> blockMetas = new ArrayList<>();
		private int totalBytes = 0;

		public void append(KeyValue lastKv, long offset, long size, byte[] bloomFilter) {
			BlockMeta blockMeta = new BlockMeta(lastKv, offset, size, bloomFilter);
			blockMetas.add(blockMeta);
			totalBytes += blockMeta.getSerializeSize();
		}

		public byte[] serialize() throws IOException {
			byte[] buffer = new byte[totalBytes];
			int pos = 0;
			for (BlockMeta blockMeta : blockMetas) {
				byte[] metaBytes = blockMeta.toBytes();
				System.arraycopy(metaBytes, 0, buffer, pos, metaBytes.length);
				pos += metaBytes.length;
			}
			assert pos == buffer.length;
			return buffer;
		}
	}

	public static class DiskFileWriter implements Closeable {
		private String fileName;

		private long currentOffset;
		private BlockIndexWriter indexWriter;
		private BlockWriter currentWriter;
		private FileOutputStream out;

		private long fileSize;
		private int blockCount;
		private long blockIndexOffset;
		private long blockIndexSize;

		public DiskFileWriter(String fileName) throws IOException {
			this.fileName = fileName;

			File f = new File(fileName);
			f.createNewFile();
			this.out = new FileOutputStream(f, true);
			this.currentOffset = 0;
			this.indexWriter = new BlockIndexWriter();
			this.currentWriter = new BlockWriter();

			this.fileSize = 0;
			this.blockCount = 0;
			this.blockIndexOffset = 0;
			this.blockIndexSize = 0;
		}

		private void switchNextBlockWriter() throws IOException {
			assert currentWriter.getLastKv() != null;
			byte[] buffer = currentWriter.serialize();
			out.write(buffer);
			indexWriter.append(currentWriter.getLastKv(),
					currentOffset, buffer.length,
					currentWriter.getBloomFilter());

			currentOffset += buffer.length;
			blockCount++;

			currentWriter = new BlockWriter();
		}

		public void append(KeyValue kv) throws IOException {
			if (kv == null) {
				return;
			}
			assert kv.getSerializedSize() +
					BlockWriter.KV_SIZE_LEN +
					BlockWriter.CHECKSUM_LEN <
					BLOCK_SIZE_UP_LIMIT;

			if (currentWriter.getKvCount() > 0 &&
					kv.getSerializedSize() + currentWriter.size() >= BLOCK_SIZE_UP_LIMIT) {
				switchNextBlockWriter();
			}

			currentWriter.append(kv);
		}

		public void appendIndex() throws IOException {
			if (currentWriter.getKvCount() > 0) {
				switchNextBlockWriter();
			}

			byte[] buffer = indexWriter.serialize();
			blockIndexOffset = currentOffset;
			blockIndexSize = buffer.length;

			out.write(buffer);
			currentOffset += blockIndexSize;
		}

		public void appendTrailer() throws IOException {
			fileSize = currentOffset + TRAILER_SIZE;
			out.write(ByteUtils.toBytes(fileSize));
			out.write(ByteUtils.toBytes(blockCount));
			out.write(ByteUtils.toBytes(blockIndexOffset));
			out.write(ByteUtils.toBytes(blockIndexSize));
			out.write(ByteUtils.toBytes(DISK_FILE_MAGIC));
		}

		@Override
		public void close() throws IOException {
			if (out != null) {
				try {
					out.flush();
					FileDescriptor fd = out.getFD();
					fd.sync();
				} finally {
					out.close();
				}
			}
		}
	}

	private static class InternalIterator implements SeekIter<KeyValue> {

		private DiskFile df;
		private int currentKvIndex = 0;
		private BlockReader currentReader;
		private KeyValueFilter filter;
		private SortedSet<BlockMeta> filteredMeta;
		private Iterator<BlockMeta> blockMetaIter;

		public InternalIterator(DiskFile df, KeyValueFilter filter) {
			this.currentReader = null;
			this.df = df;
			this.filter = filter;
			this.filteredMeta = filter.getFilteredMeta(df);
			this.blockMetaIter = filteredMeta.iterator();
		}

		private boolean nextBlockReader() throws IOException {
			if (blockMetaIter.hasNext()) {
				currentReader = df.loadReader(blockMetaIter.next());
				currentKvIndex = 0;
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void seekTo(KeyValue targetKv) throws IOException {
			blockMetaIter = filteredMeta.tailSet(BlockMeta.createSeekDummy(targetKv)).iterator();
			currentReader = null;
			while (blockMetaIter.hasNext()) {
				currentReader = df.loadReader(blockMetaIter.next());
				for (currentKvIndex = 0;
				     currentKvIndex < currentReader.getKeyValues().size();
				     currentKvIndex++) {
					KeyValue currentKv = currentReader.getKeyValues().get(currentKvIndex);
					if (currentKv.compareTo(targetKv) >= 0) {
						break;
					}
				}
				assert currentKvIndex < currentReader.getKeyValues().size();
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			if (currentReader != null &&
							currentKvIndex < currentReader.getKeyValues().size()) {
				return true;
			} else {
				return nextBlockReader();
			}
		}

		@Override
		public KeyValue next() throws IOException {
			return currentReader.getKeyValues().get(currentKvIndex++);
		}

		@Override
		public void close() {
			df.refCount.decrementAndGet();
		}
	}

	private SortedSet<BlockMeta> blockMetaSet = new TreeSet<>();
	private RandomAccessFile in;

	private final String fileName;
	private long fileSize;
	private int blockCount;
	private long blockIndexOffset;
	private long blockIndexSize;
	private final AtomicInteger refCount = new AtomicInteger(0);

	public DiskFile(String fileName) {
		this.fileName = fileName;
	}

	private BlockReader loadReader(BlockMeta meta) throws IOException {
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
		return new InternalIterator(this, filter);
	}
}
