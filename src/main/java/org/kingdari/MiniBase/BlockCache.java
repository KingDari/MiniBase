package org.kingdari.MiniBase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class BlockCache {
	private LinkedHashMap<BlockId, BlockReader> cache;
	private long capacity;

	public BlockCache(long capacity) {
		this.capacity = capacity;
		this.cache = new LinkedHashMap<BlockId, BlockReader>(64, 0.75F, true) {
			private long nowSize = 0;

			@Override
			public BlockReader put(BlockId key, BlockReader value) {
				nowSize += value.getMemorySizeSize();
				return super.put(key, value);
			}

			@Override
			protected boolean removeEldestEntry(Map.Entry<BlockId, BlockReader> eldest) {
				if (nowSize > capacity) {
					nowSize -= eldest.getValue().getMemorySizeSize();
					return true;
				} else {
					return false;
				}
			}
		};
	}

	/**
	 * Only for test.
	 */
	LinkedHashMap<BlockId, BlockReader> getCache() {
		return cache;
	}

	// TODO: Concurrent level: map -> bucket
	// TODO: RefCount: Ensure using block won't be evicted
	public synchronized BlockReader get(BlockId bid) throws IOException {
		BlockReader blockReader = cache.get(bid);
		if (blockReader == null) {
			blockReader = bid.getDiskFile().createReader(bid.getMeta());
			cache.put(bid, blockReader);
		}
		return blockReader;
	}
}
