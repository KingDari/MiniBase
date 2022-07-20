package org.kingdari.MiniBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class BlockCacheTest {

	@Test
	public void LRUTest() {
		DiskFile mockFileA = new DiskFile("a");
		DiskFile mockFileB = new DiskFile("b");
		BlockMeta mockMeta1 = new BlockMeta(
				KeyValue.createPut(new byte[]{1}, new byte[]{1}, 1L), 0, 0, new byte[0]);
		BlockMeta mockMeta2 = new BlockMeta(
				KeyValue.createPut(new byte[]{1}, new byte[]{1}, 2L), 0, 0, new byte[0]);
		BlockId id1 = new BlockId(mockFileA, mockMeta1);
		BlockId id2 = new BlockId(mockFileA, mockMeta2);
		BlockId id3 = new BlockId(mockFileB, mockMeta1);
		BlockId id4 = new BlockId(mockFileB, mockMeta2);
		List<KeyValue> mockList = new ArrayList<>();
		// 1 + 1 + 1 + 8 = 11
		mockList.add(KeyValue.createPut(new byte[]{1}, new byte[]{1}, 1L));
		BlockReader mockReader = new BlockReader(mockList);

		LinkedHashMap<BlockId, BlockReader> cache = new BlockCache(32L).getCache();
		cache.put(id1, mockReader);
		cache.put(id2, mockReader);
		cache.put(id3, mockReader);
		cache.put(id4, mockReader);
		Assertions.assertEquals(cache.size(), 2);
		Assertions.assertNull(cache.get(id1));
		Assertions.assertNull(cache.get(id2));
		Assertions.assertEquals(cache.get(id3), mockReader);
		Assertions.assertEquals(cache.get(id4), mockReader);
		cache.get(id3);
		cache.put(id1, mockReader);
		Assertions.assertEquals(cache.size(), 2);
		Assertions.assertNull(cache.get(id2));
		Assertions.assertNull(cache.get(id4));
		Assertions.assertEquals(cache.get(id1), mockReader);
		Assertions.assertEquals(cache.get(id3), mockReader);
	}

	@Test
	public void evictTest() {
		LinkedHashMap<BlockId, BlockReader> cache = new BlockCache(32L).getCache();
		for (int i = 0; i < 512; i++) {
			DiskFile mockFile = new DiskFile("mock");
			BlockMeta mockMeta = new BlockMeta(
					KeyValue.createPut(new byte[]{1}, new byte[]{1}, i), 0, 0, new byte[0]);
			List<KeyValue> mockList = new ArrayList<>();
			// 1 + 1 + 1 + 8 = 11
			mockList.add(KeyValue.createPut(new byte[]{1}, new byte[]{1}, 1L));
			cache.put(new BlockId(mockFile, mockMeta), new BlockReader(mockList));
		}
		Assertions.assertEquals(cache.size(), 2);
	}

	@Test
	public void zeroTest() {
		LinkedHashMap<BlockId, BlockReader> cache = new BlockCache(16L).getCache();
		for (int i = 0; i < 512; i++) {
			DiskFile mockFile = new DiskFile("mock");
			BlockMeta mockMeta = new BlockMeta(
					KeyValue.createPut(new byte[]{1}, new byte[]{1}, i), 0, 0, new byte[0]);
			cache.put(new BlockId(mockFile, mockMeta), new BlockReader(new ArrayList<>()));
		}
		Assertions.assertEquals(cache.size(), 512);
	}
}
