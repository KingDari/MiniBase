package org.kingdari.MiniBase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.kingdari.MiniBase.DiskFile.BLOOM_FILTER_BITS_PER_KEY;
import static org.kingdari.MiniBase.DiskFile.BLOOM_FILTER_HASH_COUNT;

public class BlockWriter {
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
