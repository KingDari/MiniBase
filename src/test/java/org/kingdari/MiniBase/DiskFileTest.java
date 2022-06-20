package org.kingdari.MiniBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.kingdari.MiniBase.DiskFile.BlockMeta;
import org.kingdari.MiniBase.DiskFile.BlockReader;
import org.kingdari.MiniBase.DiskFile.BlockWriter;
import org.kingdari.MiniBase.DiskFile.DiskFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class DiskFileTest {
	public static final Random RANDOM = new Random();

	@Test
	public void blockEncodingTest() throws IOException {
		BlockWriter bw = new BlockWriter();
		byte[] lastBytes = null;
		int i;
		for (i = 0; i < 100; i++) {
			lastBytes = ByteUtils.toBytes(i);
			bw.append(KeyValue.createPut(lastBytes, lastBytes, i));
		}
		Assertions.assertEquals(bw.getLastKv(), KeyValue.createPut(lastBytes, lastBytes, i - 1));

		byte[] buffer = bw.serialize();
		BlockReader br = BlockReader.parseFrom(bw.serialize(), 0, buffer.length);

		byte[][] keys = new byte[br.getKeyValues().size()][];
		for (int j = 0; j < keys.length; j++) {
			keys[j] = br.getKeyValues().get(j).getKey();
		}
		BloomFilter bloomFilter = new BloomFilter(
				DiskFile.BLOOM_FILTER_HASH_COUNT, DiskFile.BLOOM_FILTER_BITS_PER_KEY);
		Assertions.assertArrayEquals(bloomFilter.generate(keys), bw.getBloomFilter());
	}

	@Test
	public void blockMetaTest() throws IOException {
		KeyValue lastKv = KeyValue.createPut(
				ByteUtils.toBytes("abc"), ByteUtils.toBytes("abc"), 1L);
		long offset = 1024, size = 1024;
		byte[] bloomFilter = ByteUtils.toBytes("bloomFilter");

		BlockMeta meta = new BlockMeta(lastKv, offset, size, bloomFilter);
		byte[] buffer = meta.toBytes();

		BlockMeta meta2 = BlockMeta.parseFrom(buffer, 0);
		Assertions.assertEquals(lastKv, meta2.getLastKv());
		Assertions.assertEquals(offset, meta2.getBlockOffset());
		Assertions.assertEquals(size, meta2.getBlockSize());
		Assertions.assertArrayEquals(bloomFilter, meta2.getBFBytes());
	}

	private byte[] generateRandomBytes() {
		int len = (RANDOM.nextInt() % 1024 + 1024) % 1024;
		byte[] buffer = new byte[len];
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = (byte) (RANDOM.nextInt() & 0xFF);
		}
		return buffer;
	}

	@Test
	public void diskFileTest() throws IOException {
		String dbFile = "diskFileTest.db";
		try {
			try (DiskFileWriter dfw = new DiskFileWriter(dbFile)) {
				for (int i = 0; i < 1000; i++) {
					dfw.append(KeyValue.createPut(
							generateRandomBytes(), generateRandomBytes(), 1L));
				}
				dfw.appendIndex();
				dfw.appendTrailer();
			}
			try (DiskFile df = new DiskFile(dbFile)){
				df.open();
			}
		} finally {
			File f = new File(dbFile);
			if (f.exists()) {
				f.delete();
			}
		}
	}

	@Test
	public void diskFileIOTest() throws IOException {
		String dbFile = "diskFileIOTest.db";
		int rowsCount = 1000;

		try {
			DiskFileWriter dfw = new DiskFileWriter(dbFile);

			for (int i = 0; i < rowsCount; i++) {
				dfw.append(KeyValue.createPut(
						ByteUtils.toBytes(i), ByteUtils.toBytes(i), 1L));
			}

			dfw.appendIndex();
			dfw.appendTrailer();
			dfw.close();

			try (DiskFile df = new DiskFile(dbFile).open()) {
				try (MStore.SeekIter<KeyValue> iter = df.iterator()) {
					int index = 0;
					while (iter.hasNext()) {
						KeyValue kv = iter.next();
						Assertions.assertEquals(kv, KeyValue.createPut(
								ByteUtils.toBytes(index), ByteUtils.toBytes(index), 1L));
						index++;
					}
					Assertions.assertEquals(index, rowsCount);
				}
			}

			try (DiskFile df = new DiskFile(dbFile).open()) {
				try (MStore.SeekIter<KeyValue> iter = df.iterator()) {
					int index = rowsCount / 5;
					iter.seekTo(KeyValue.createPut(
							ByteUtils.toBytes(index), ByteUtils.toBytes(index), 1L));
					while (iter.hasNext()) {
						KeyValue kv = iter.next();;
						Assertions.assertEquals(kv, KeyValue.createPut(
								ByteUtils.toBytes(index), ByteUtils.toBytes(index), 1L));
						index++;
					}
					Assertions.assertEquals(index, rowsCount);
				}
			}
		} finally {
			File f = new File(dbFile);
			if (f.exists()) {
				f.delete();
			}
		}
	}
}
