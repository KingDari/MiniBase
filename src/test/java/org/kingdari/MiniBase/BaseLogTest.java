package org.kingdari.MiniBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.kingdari.MiniBase.Config.WAL_LEVEL;

import java.util.concurrent.BlockingQueue;

public class BaseLogTest {

	private void basicHelper(WAL_LEVEL walLevel) throws Exception {
		Config conf = Config.getDefault().setWalLevel(walLevel);
		BaseLog log = new BaseLog(conf);
		for (int i = 0; i <= 16; i++) {
			log.put(ByteUtils.toBytes(i), ByteUtils.toBytes(i));
		}
		log.close();
		final BlockingQueue<LogEntry> queue = log.getReadQueue();
		for (int i = 0; i <= 16; i++) {
			Assertions.assertEquals(((LogWal) queue.take()).getKv(),
					KeyValue.createPut(ByteUtils.toBytes(i), ByteUtils.toBytes(i), i + 1));
		}
	}

	private void concurrentHelper(WAL_LEVEL walLevel) throws Exception {
		Config conf = Config.getDefault().setWalLevel(walLevel);
		BaseLog log = new BaseLog(conf);
		int n = 1000, ths = 4;
		Thread[] threads = new Thread[ths];
		for (int i = 0; i < ths; i++) {
			final int fi = i;
			threads[i] = new Thread(() -> {
				for (int j = n / ths * fi; j < n / ths * (fi + 1); j++) {
					log.put(ByteUtils.toBytes(j), ByteUtils.toBytes(j));
				}
			}, "Put thread-" + i);
			threads[i].start();
		}
		for (int i = 0; i < ths; i++) {
			threads[i].join();
		}
		log.close();
		final BlockingQueue<LogEntry> queue = log.getReadQueue();
		boolean[] checkSeq = new boolean[n];
		boolean[] checkKey = new boolean[n];
		for (int i = 0; i < n; i++) {
			LogWal wal = (LogWal) queue.take();
			KeyValue kv = wal.getKv();
			Assertions.assertArrayEquals(kv.getKey(), kv.getValue());
			Assertions.assertEquals(kv.getOp(), KeyValue.Op.Put);
			Assertions.assertFalse(checkSeq[(int) kv.getSequenceId() - 1]);
			Assertions.assertFalse(checkKey[ByteUtils.toInt(kv.getKey())]);
			checkSeq[(int) kv.getSequenceId() - 1] = true;
			checkKey[ByteUtils.toInt(kv.getKey())] = true;
		}
	}

	@Test
	public void basicTest() throws Exception {
		basicHelper(WAL_LEVEL.SKIP);
		basicHelper(WAL_LEVEL.ASYNC);
		basicHelper(WAL_LEVEL.SYNC);
		basicHelper(WAL_LEVEL.FSYNC);
	}

	@Test
	public void concurrentTest() throws Exception {
		concurrentHelper(WAL_LEVEL.SKIP);
		concurrentHelper(WAL_LEVEL.ASYNC);
		concurrentHelper(WAL_LEVEL.SYNC);
		concurrentHelper(WAL_LEVEL.FSYNC);
	}
}
