package org.kingdari.MiniBase;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.kingdari.MiniBase.DiskFile.BLOCK_SIZE_UP_LIMIT;
import static org.kingdari.MiniBase.DiskFile.DISK_FILE_MAGIC;
import static org.kingdari.MiniBase.DiskFile.TRAILER_SIZE;

public class DiskFileWriter implements Closeable {

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
