package org.kingdari.MiniBase;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class BloomFilterTest {
	private static final Logger LOG = Logger.getLogger(BloomFilterTest.class);

	@Test
	public void bloomFilterTest() {
		String[] keys = "hello bloomFilter, this is a_test for U".split(" ");
		BloomFilter bloomFilter = new BloomFilter(5, 90);
		byte[][] keyBytes = new byte[keys.length][];
		for (int i = 0; i < keys.length; i++) {
			keyBytes[i] = keys[i].getBytes(StandardCharsets.UTF_8);
		}
		bloomFilter.generate(keyBytes);
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("hello")));
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("bloomFilter,")));
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("this")));
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("is")));
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("a_test")));
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("for")));
		Assertions.assertTrue(bloomFilter.mayContains(ByteUtils.toBytes("U")));

		int incorrectClassifyCount = 0, maxClassify = 10000;
		for (int i = 0; i < maxClassify; i++) {
			String randomString = UUID.randomUUID().toString();
			if (bloomFilter.mayContains(ByteUtils.toBytes(randomString))) {
				incorrectClassifyCount++;
			}
		}

		System.out.printf(
				"the probability of incorrect classify is %.2g\n",
				((double) incorrectClassifyCount) / maxClassify);
	}
}
