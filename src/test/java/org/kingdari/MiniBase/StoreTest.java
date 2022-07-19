package org.kingdari.MiniBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kingdari.MiniBase.MStore.ScanIter;
import org.kingdari.MiniBase.MStore.SeekIter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StoreTest {

	private static class WriterThread extends Thread {
		private long start, end;
		private Store db;
		private long retryCount;

		public WriterThread(Store db, long start, long end) {
			this.db = db;
			this.start = start;
			this.end = end;
			this.retryCount = 0L;
		}

		@Override
		public void run() {
			for (long i = start; i < end; i++) {
				int retries = 0;
				boolean success = false;
				while (retries < 10) {
					try {
						db.put(ByteUtils.toBytes(i), ByteUtils.toBytes(i));
						success = true;
						break;
					} catch (IOException e) {
						retries++;
						try {
							Thread.sleep(100L * (1L << retries));
						} catch (InterruptedException ignored) {
						}
					}
				}
				if (!success) {
					Assertions.fail("Put fail");
				}
				retryCount += retries;
			}
		}
	}

	public static class MockIter implements SeekIter<KeyValue> {
		private int cur;
		private KeyValue[] kvs;

		public MockIter(List<KeyValue> array) throws IOException {
			assert array != null;
			this.kvs = array.toArray(new KeyValue[0]);
			this.cur = 0;
		}

		@Override
		public void seekTo(KeyValue kv) throws IOException {
			for (cur = 0; cur < kvs.length; cur++) {
				if (kvs[cur].compareTo(kv) >= 0) {
					break;
				}
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			return cur < kvs.length;
		}

		@Override
		public KeyValue next() throws IOException {
			return kvs[cur++];
		}

		@Override
		public void close() {

		}
	}

	private String rootDir;

	@BeforeEach
	public void setUp() {
		this.rootDir = "output/MiniBase-" + System.currentTimeMillis();
	}

	private void deleteDir(File dir) {
		if (!dir.exists()) {
			return;
		}
		for (File file : dir.listFiles()) {
			if (file.isFile()) {
				file.delete();
			} else if (file.isDirectory()) {
				deleteDir(file);
			}
		}
		dir.delete();
	}

	@AfterEach
	public void tearDown() {
		deleteDir(new File(rootDir));
	}

	@Test
	public void putTest() throws IOException, InterruptedException {
		Config conf = new Config().
				setRoorDir(rootDir).
				setMaxMemStoreSize(512). // Make flushing and compacting frequently
				setFlushMaxRetryTimes(3).
				setMaxDiskFiles(10).
				setWalLevel(Config.WAL_LEVEL.SKIP);
		final Store db = MStore.create(conf).open();

		final long totalKvSize = 1000L;
		final int threadSize = 5;

		WriterThread[] writers = new WriterThread[threadSize];
		for (int i = 0; i < threadSize; i++) {
			long kvPerThread = totalKvSize / threadSize;
			writers[i] = new WriterThread(db, i * kvPerThread, (i + 1) * kvPerThread);
			writers[i].start();
		}

		for (WriterThread writer : writers) {
			writer.join();
		}

		try (Store.Iter<KeyValue> iter = db.scan()) {
			long current = 0;
			while (iter.hasNext()) {
				KeyValue expected = iter.next();
				KeyValue currentKv = KeyValue.createPut(
						ByteUtils.toBytes(current), ByteUtils.toBytes(current), 0L);
				Assertions.assertArrayEquals(expected.getKey(), currentKv.getKey());
				Assertions.assertArrayEquals(expected.getValue(), currentKv.getValue());
				Assertions.assertEquals(expected.getOp(), currentKv.getOp());

				long seqId = expected.getSequenceId();
				Assertions.assertTrue(seqId > 0);
				current++;
			}
			Assertions.assertEquals(current, totalKvSize);
		}

		// mvcc
		try (Store.Iter<KeyValue> iter = db.scan(new KeyValueFilter().setVersion(500L))) {
			int count = 0;
			while (iter.hasNext()) {
				Assertions.assertTrue(iter.next().getSequenceId() <= 500L);
				count++;
			}
			Assertions.assertEquals(count, 500);
		}
		db.close();
	}

	@Test
	public void mixedOpTest() throws Exception {
		Config conf = new Config().
				setRoorDir(rootDir).
				setMaxMemStoreSize(128).
				setWalLevel(Config.WAL_LEVEL.SYNC);
		final Store db = MStore.create(conf).open();

		byte[] A = ByteUtils.toBytes("A");
		byte[] B = ByteUtils.toBytes("B");
		byte[] C = ByteUtils.toBytes("C");

		KeyValueFilter filterA = new KeyValueFilter().setKey(A).setVersion(100L);
		KeyValueFilter filterB = new KeyValueFilter().setKey(B).setVersion(100L);
		KeyValueFilter filterC = new KeyValueFilter().setKey(C).setVersion(100L);

		db.put(A, A);
		Assertions.assertArrayEquals(db.get(filterA).getValue(), A);
		db.put(A, B);
		Assertions.assertArrayEquals(db.get(filterA).getValue(), B);
		db.delete(A);
		Assertions.assertNull(db.get(filterA));
		db.put(B, A);
		db.put(A, B);
		Assertions.assertArrayEquals(db.get(filterA).getValue(), B);
		Assertions.assertArrayEquals(db.get(filterB).getValue(), A);
		db.delete(B);
		db.put(C, C);
		Assertions.assertArrayEquals(db.get(filterA).getValue(), B);
		Assertions.assertNull(db.get(filterB));
		Assertions.assertArrayEquals(db.get(filterC).getValue(), C);
		db.close();
	}

	@Test
	public void scanIterTest() throws IOException {
		List<KeyValue> list = new ArrayList<>();
		byte[] A = ByteUtils.toBytes("A");
		byte[] B = ByteUtils.toBytes("B");
		byte[] C = ByteUtils.toBytes("C");

		list.add(KeyValue.createDelete(A, 101));
		list.add(KeyValue.createDelete(A, 101));
		list.add(KeyValue.createPut(A, A, 100));
		list.add(KeyValue.createPut(A, A, 99));

		list.add(KeyValue.createPut(B, C, 98));
		list.add(KeyValue.createPut(B, B, 97));
		list.add(KeyValue.createPut(B, B, 97));
		list.add(KeyValue.createPut(B, A, 96));

		list.add(KeyValue.createPut(C, C, 102));
		list.add(KeyValue.createDelete(C, 1));

		ScanIter scan = new ScanIter(null, new MockIter(list));
		Assertions.assertTrue(scan.hasNext());
		Assertions.assertEquals(scan.next(), KeyValue.createPut(B, C, 98));
		Assertions.assertTrue(scan.hasNext());
		Assertions.assertEquals(scan.next(), KeyValue.createPut(C, C, 102));
		Assertions.assertFalse(scan.hasNext());

		scan = new ScanIter(KeyValue.createPut(B, C, 98), new MockIter(list));
		Assertions.assertFalse(scan.hasNext());

		scan = new ScanIter(KeyValue.createPut(C, C, 102), new MockIter(list));
		Assertions.assertTrue(scan.hasNext());
		Assertions.assertEquals(scan.next(), KeyValue.createPut(B, C, 98));
		Assertions.assertFalse(scan.hasNext());
	}
}
