package org.kingdari.MiniBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.kingdari.MiniBase.Config.WAL_LEVEL;

import java.io.Closeable;
import java.io.IOException;

public class BaseLogTest {

	static class MockLogConsumer extends Thread implements Closeable {

		private BaseLog log;
		private long expectSeq;
		private volatile boolean running;

		MockLogConsumer(BaseLog log) {
			this.log = log;
			this.running = true;
			this.expectSeq = 1;
		}

		public long getExpectSeq() {
			return expectSeq;
		}

		@Override
		public void run() {
			while (running || !log.getReadQueue().isEmpty()) {
				try {
					LogEntry entry = log.getReadQueue().take();
					long seq = ((ReadLogWal) entry).getKv().getSequenceId();
					Assertions.assertEquals(seq, expectSeq);
					expectSeq++;
					log.notifyLogEntry(seq);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void close() throws IOException {
			this.running = false;
		}
	}

	private void basicHelper(WAL_LEVEL walLevel) throws Exception {
		Config conf = Config.getDefault().setWalLevel(walLevel);
		BaseLog log = new BaseLog(conf);
		MockLogConsumer consumer = new MockLogConsumer(log);
		consumer.start();
		for (int i = 0; i <= 16; i++) {
			log.put(ByteUtils.toBytes(i), ByteUtils.toBytes(i));
		}
		log.close();
		consumer.close();
		Assertions.assertEquals(consumer.getExpectSeq(), 18);
	}

	private void concurrentHelper(WAL_LEVEL walLevel) throws Exception {
		Config conf = Config.getDefault().setWalLevel(walLevel);
		BaseLog log = new BaseLog(conf);
		MockLogConsumer consumer = new MockLogConsumer(log);
		consumer.start();
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
		consumer.close();
		Assertions.assertEquals(consumer.getExpectSeq(), 1001);
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
