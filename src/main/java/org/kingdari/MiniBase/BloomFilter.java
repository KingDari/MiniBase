package org.kingdari.MiniBase;

public class BloomFilter {
	private int k;
	private int bitsPerKey;
	private int bitLen;
	private byte[] result;

	public BloomFilter(int bloomFilterHashCount, int bloomFilterBitsPerKey) {
		this.k = bloomFilterHashCount;
		this.bitsPerKey = bloomFilterHashCount;
	}

	public byte[] generate(byte[][] keys) {
		assert keys != null;
		bitLen = bitsPerKey * keys.length;
		bitLen = ((bitLen + 7) >> 3) << 3;
		bitLen = Math.max(bitLen, 64);
		result = new byte[bitLen >> 3];
		for (byte[] key : keys) {
			assert key != null;
			int h = ByteUtils.hash(key);
			for (int i = 0; i < k; i++) {
				int idx = (h % bitLen + bitLen) % bitLen;
				result[idx / 8] |= (1 << (idx % 8));
				int delta = (h >> 17) | (h << 15);
				h += delta;
			}
		}
		return result;
	}

	public boolean mayContains(byte[] key) {
		assert key != null;
		int h = ByteUtils.hash(key);
		for (int i = 0; i < k; i++) {
			int idx = (h % bitLen + bitLen) % bitLen;
			if ((result[idx / 8] & (1 << (idx % 8))) == 0) {
				return false;
			}
			int delta = (h >> 17) | (h << 15);
			h += delta;
		}
		return true;
	}
}
