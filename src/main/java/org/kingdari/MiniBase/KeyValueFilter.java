package org.kingdari.MiniBase;

import java.util.SortedSet;
import java.util.TreeSet;

public class KeyValueFilter {

	public static KeyValueFilter createEmptyFilter() {
		return new KeyValueFilter();
	}

	private byte[] start;
	private byte[] end;
	private byte[] key;
	private long version;

	public KeyValueFilter() {
		this.start = ByteUtils.EMPTY_BYTES;
		this.end = ByteUtils.EMPTY_BYTES;
		this.key = null;
		this.version = Long.MAX_VALUE;
	}

	public byte[] getStart() {
		return start;
	}

	public byte[] getEnd() {
		return end;
	}

	public KeyValueFilter setRange(byte[] start, byte[] end) {
		this.start = start;
		this.end = end;
		return this;
	}

	public boolean isVisible(KeyValue kv) {
		return version >= kv.getSequenceId();
	}

	public KeyValueFilter setVersionIfAbsent(long version) {
		this.version = this.version == Long.MAX_VALUE ? version : this.version;
		return this;
	}

	public KeyValueFilter setVersion(long version) {
		this.version = version;
		return this;
	}

	public long getVersion() {
		return version;
	}

	public byte[] getKey() {
		return key;
	}

	public KeyValueFilter setKey(byte[] key) {
		this.key = key;
		return this;
	}

	public SortedSet<BlockMeta> getFilteredMeta(DiskFile df) {
		if (key == null) {
			return df.getBlockMetaSet();
		}
		SortedSet<BlockMeta> ret = new TreeSet<>();
		BloomFilter bf = new BloomFilter(
				DiskFile.BLOOM_FILTER_HASH_COUNT,
				DiskFile.BLOOM_FILTER_BITS_PER_KEY);
		for (BlockMeta blockMeta : df.getBlockMetaSet()) {
			bf.setResult(blockMeta.getBFBytes());
			if (key == ByteUtils.EMPTY_BYTES || bf.mayContains(key)) {
				ret.add(blockMeta);
			}
		}
		return ret;
	}
}
