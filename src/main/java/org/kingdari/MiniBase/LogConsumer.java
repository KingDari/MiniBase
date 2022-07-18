package org.kingdari.MiniBase;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

public class LogConsumer extends Thread implements Closeable {

	private static final Logger LOG = Logger.getLogger(LogConsumer.class);

	private Config conf;
	private BaseLog log;
	private MemStore memStore;
	private MStore store;

	private volatile boolean running;

	public LogConsumer(Config conf, BaseLog log, MemStore memStore, MStore store) {
		super("LogConsumer");
		this.conf = conf;
		this.log = log;
		this.memStore = memStore;
		this.store = store;
		this.running = true;
		this.setDaemon(true);
	}

	@Override
	public void run() {
		while (running) {
			try {
				LogEntry entry = log.getReadQueue().take();
				if (entry instanceof ReadLogWal) {
					int i = 0;
					for ( ; i < conf.getPutMaxRetryTimes(); i++) {
						try {
							KeyValue kv = ((ReadLogWal) entry).getKv();
							memStore.add(kv);
							long seq = kv.getSequenceId();
							long after = store.updateGlobalVersion(seq);
							LOG.debug("Get KV: " + kv + ". Try to update global version: " + seq + ", result: " + after);
							log.notifyLogEntry(seq);
							break;
						} catch (IOException e) {
							LOG.warn("Put KV failed: " + e.getMessage());
							try {
								Thread.sleep(100L << i);
							} catch (InterruptedException ignored) {
							}
						}
					}
					if (i == conf.getPutMaxRetryTimes()) {
						LOG.error("Put failed.");
					}
				} else {
					LOG.error("Not supported LogEntry");
				}
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
