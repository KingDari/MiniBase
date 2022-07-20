package org.kingdari.MiniBase;


import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class BlockMeta implements Comparable<BlockMeta> {

	private static final int OFFSET_SIZE = 8;
	private static final int SIZE_SIZE = 8;
	private static final int BF_LEN_SIZE = 4;

	static BlockMeta createSeekDummy(KeyValue lastKv) {
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BlockMeta blockMeta = (BlockMeta) o;
		return blockOffset == blockMeta.blockOffset && blockSize == blockMeta.blockSize && Objects.equals(lastKv, blockMeta.lastKv) && Arrays.equals(bfBytes, blockMeta.bfBytes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(lastKv, blockOffset, blockSize);
		result = 31 * result + Arrays.hashCode(bfBytes);
		return result;
	}
}
