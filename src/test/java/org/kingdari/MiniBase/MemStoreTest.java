package org.kingdari.MiniBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MemStoreTest {
	private static class SleepAndFlusher implements Store.Flusher {
		private volatile boolean sleepNow = true;

		@Override
		public void flush(Store.Iter<KeyValue> iter) throws IOException {
			while(sleepNow) {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException ignored) {
				}
			}
		}

		public void stopSleeping() {
			sleepNow = false;
		}
	}

	@Test
	public void blockingPutTest() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(1);
		try {
			Config conf = new Config().setMaxMemStoreSize(26).setFlushMaxRetryTimes(5);
			SleepAndFlusher flusher = new SleepAndFlusher();
			MemStore memStore = new MemStore(conf, flusher, pool);
			memStore.add(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(1), 1L));
			// RawKeyLen, ValLen, key, val, seq, op
			// 4, 4, 4, 4, 8, 1
			Assertions.assertEquals(memStore.getDataSize(), 25);
			memStore.add(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(2), 2L));
			Assertions.assertEquals(memStore.getDataSize(), 0);

			// Flushing... (Block by SleepFlusher)
			memStore.add(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(3), 3L));
			memStore.add(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(4), 4L));
			try {
				memStore.add(KeyValue.createPut(
						ByteUtils.toBytes(1), ByteUtils.toBytes(5), 5L));
				Assertions.fail("Should catch exception");
			} catch (IOException e) {
				Assertions.assertTrue(e.getMessage().contains("MemStore is full"));
				Assertions.assertTrue(memStore.isFlushing());
				Assertions.assertEquals(memStore.getDataSize(), 50);
			}
			flusher.stopSleeping();
			Thread.sleep(200); // wait flusher exit
			Assertions.assertFalse(memStore.isFlushing());
			Assertions.assertEquals(memStore.getDataSize(), 50);
			memStore.add(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(5), 5L));
			Assertions.assertEquals(memStore.getDataSize(), 25);

			ConcurrentSkipListMap<KeyValue, KeyValue> map = memStore.getMemStoreMaps()[0];
			Assertions.assertTrue(map.containsKey(
					KeyValue.createPut(ByteUtils.toBytes(1), ByteUtils.toBytes(5), 5L)));
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	public void putDeleteTest() throws IOException {
		ExecutorService pool = Executors.newFixedThreadPool(1);
		try {
			Config conf = new Config().setMaxMemStoreSize(2 * 1024 * 1024);
			MemStore memStore = new MemStore(conf, new SleepAndFlusher(), pool);
			for (int i = 99; i >= 0; i--) {
				KeyValue kv;
				byte[] bytes = ByteUtils.toBytes(i);
				if ((i & 1) == 0) {
					kv = KeyValue.createPut(bytes, bytes, i);
				} else {
					kv = KeyValue.createDelete(bytes, i);
				}
				memStore.add(kv);
			}
			final MStore.SeekIter<KeyValue> iter = memStore.createIterator();
			int index = 0;
			while (iter.hasNext()) {
				KeyValue kv = iter.next();
				byte[] bytes = ByteUtils.toBytes(index);
				if ((index & 1) == 0) {
					Assertions.assertEquals(kv, KeyValue.createPut(bytes, bytes, index));
				} else {
					Assertions.assertEquals(kv, KeyValue.createDelete(bytes, index));
				}
				index++;
			}
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	public void seqIdAndOpOrderTest() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(1);
		try {
			Config conf = new Config().setMaxMemStoreSize(2 * 1024 * 1024);
			MemStore memStore = new MemStore(conf, new SleepAndFlusher(), pool);
			byte[] bytes = ByteUtils.toBytes(1);

			KeyValue kv1 = KeyValue.createPut(bytes, bytes, 1);
			KeyValue kv2 = KeyValue.createDelete(bytes, 2);

			memStore.add(kv1);
			final MStore.SeekIter<KeyValue> iter1 = memStore.createIterator();
			Assertions.assertEquals(iter1.next(), kv1);
			Assertions.assertFalse(iter1.hasNext());
			memStore.add(kv2);
			final MStore.SeekIter<KeyValue> iter2 = memStore.createIterator();
			Assertions.assertEquals(iter2.next(), kv2);
			Assertions.assertEquals(iter2.next(), kv1);
			Assertions.assertFalse(iter1.hasNext());
			Assertions.assertFalse(iter2.hasNext());
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	public void mvccMemStoreTest() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(1);
		try {
			Config conf = new Config().setMaxMemStoreSize(2 * 1024 * 1024);
			MemStore memStore = new MemStore(conf, new SleepAndFlusher(), pool);
			byte[] bytes = ByteUtils.toBytes(1);

			KeyValue kv1 = KeyValue.createPut(bytes, bytes, 1);
			KeyValue kv2 = KeyValue.createDelete(bytes, 2);

			memStore.add(kv1);
			MStore.SeekIter<KeyValue> iter1 = memStore.createIterator(new KeyValueFilter().setVersion(0L));
			Assertions.assertFalse(iter1.hasNext());
			iter1 = memStore.createIterator(new KeyValueFilter().setVersion(1L));
			Assertions.assertTrue(iter1.hasNext());
			Assertions.assertEquals(iter1.next(), kv1);
			iter1 = memStore.createIterator(new KeyValueFilter().setVersion(1000L));
			Assertions.assertTrue(iter1.hasNext());
			Assertions.assertEquals(iter1.next(), kv1);
			memStore.add(kv2);
			MStore.SeekIter<KeyValue> iter2 = memStore.createIterator(new KeyValueFilter().setVersion(2L));
			Assertions.assertTrue(iter2.hasNext());
			Assertions.assertEquals(iter2.next(), kv2);
			Assertions.assertTrue(iter2.hasNext());
			Assertions.assertEquals(iter2.next(), kv1);
		} finally {
			pool.shutdownNow();
		}
	}
}
