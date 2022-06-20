package org.kingdari.MiniBase;

import org.kingdari.MiniBase.DiskFile.BlockMeta;

import java.util.SortedSet;
import java.util.TreeSet;

public class KeyValueFilter {
	private byte[] key;

	public KeyValueFilter() {
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
