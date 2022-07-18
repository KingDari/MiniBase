package org.kingdari.MiniBase;

import org.apache.log4j.Logger;
import org.kingdari.MiniBase.Config.WAL_LEVEL;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BaseLog implements MLog {

	private static final Logger LOG = Logger.getLogger(BaseLog.class);

	private class ReadQueueProducer extends Thread implements Closeable {

		private LogReader logReader;
		private volatile boolean running;
		private int readPos;

		public ReadQueueProducer() throws IOException {
			super("BaseLogReadQueueProducer");
			this.logReader = new LogReader(logFileName);
			this.running = true;
			this.readPos = 0;
			this.setDaemon(true);
		}

		@Override
		public void run() {
			while (running) {
				synchronized (logCurrPos) {
					while (running && readPos == logCurrPos.get()) {
						try {
							logCurrPos.wait();
						} catch (InterruptedException ignored) {
						}
					}
				}
				int newPos = logCurrPos.get();
				try {
					final List<LogEntry> entries = logReader.read(readPos, newPos - readPos);
					readQueue.addAll(entries);
					readPos = newPos;
				} catch (IOException e) {
					LOG.error("Read log failed", e);
				}
			}
		}

		@Override
		public void close() throws IOException {
			this.running = false;
		}
	}

	private class WriteQueueConsumer extends Thread implements Closeable {

		private LogWriter logWriter;
		private volatile boolean running;

		public WriteQueueConsumer() throws IOException {
			super("BaseLogWriteQueueConsumer");
			this.logWriter = new LogWriter(logFileName, conf.getLogBufferSize(), conf.getWalLevel(), logCurrPos);
			this.running = true;
			this.setDaemon(true);
		}

		@Override
		public void run() {
			while (running || !writeQueue.isEmpty()) {
				try {
					LogEntry logEntry = writeQueue.take();
					if (logEntry instanceof WriteLogWal) {
						WriteLogWal entry = (WriteLogWal) logEntry;
						long seq = sequenceId.incrementAndGet();
						entry.setSeq(seq);
						seqIdToEntry.put(seq, entry);

						if (conf.getWalLevel() == WAL_LEVEL.SKIP) {
							readQueue.add(entry.toReadLogWal());
						} else if (conf.getWalLevel() == WAL_LEVEL.ASYNC) {
							readQueue.add(entry.toReadLogWal());
							logWriter.append(entry);
						} else if (conf.getWalLevel() == WAL_LEVEL.SYNC || conf.getWalLevel() == WAL_LEVEL.FSYNC) {
							logWriter.append(entry);
							logWriter.sync();
						}

					} else if (logEntry instanceof LogSync) {
						logWriter.sync();
					}
				} catch (InterruptedException | IOException e) {
					e.printStackTrace();
				}
			}
			try {
				logWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void close() throws IOException {
			this.running = false;
		}
	}

	/**
	 * Non-concurrent
	 */
	private static class LogReader implements Closeable {

		private String logFileName;
		private FileChannel channel;
		private int pos;

		public LogReader(String logFileName) throws IOException {
			this.logFileName = logFileName;
			this.channel = new FileInputStream(logFileName).getChannel();
			this.pos = 0;
		}

		public List<LogEntry> read(int offset, int len) throws IOException {
			List<LogEntry> entries = new ArrayList<>();
			ByteBuffer buffer = ByteBuffer.allocateDirect(len);
			int l = channel.read(buffer, offset);
			assert l == len;
			byte[] buf = new byte[len];
			buffer.rewind();
			buffer.get(buf);

			int pos = 0;
			while (pos != len) {
				// TODO: factory class
				LogEntry entry = ReadLogWal.parseFrom(buf, pos);
				pos += entry.getSerializedSize();
				entries.add(entry);
				assert pos <= len;
			}
			return entries;
		}

		@Override
		public void close() throws IOException {
			channel.close();
		}
	}

	/**
	 * Non-concurrent
	 */
	private static class LogWriter implements Closeable {

		private String logFileName;
		private final AtomicInteger currPos;
		private FileChannel channel;
		private ByteBuffer buffer;
		private WAL_LEVEL walLevel;
		private int maxSize;
		private int size;

		public LogWriter(String logFileName, int bufferSize, WAL_LEVEL walLevel, AtomicInteger currPos) throws IOException {
			this.logFileName = logFileName;
			this.currPos = currPos;
			File f = new File(logFileName);
			f.delete();
			f.createNewFile();
			this.channel = new FileOutputStream(f, false).getChannel();
			this.buffer = ByteBuffer.allocateDirect(bufferSize);
			this.maxSize = bufferSize;
			this.size = 0;
			this.walLevel = walLevel;
		}

		public void append(WriteLogWal logWal) throws IOException {
			if (!channel.isOpen()) {
				return;
			}
			if (size + logWal.getSerializedSize() > maxSize) {
				sync();
			}
			size += logWal.getSerializedSize();
			buffer.put(logWal.toBytes());
		}

		public void sync() throws IOException {
			if (!channel.isOpen()) {
				return;
			}
			int pos = buffer.position();
			buffer.flip();
			while (buffer.hasRemaining()) {
				channel.write(buffer);
			}
			if (walLevel == WAL_LEVEL.FSYNC) {
				channel.force(true);
			}
			long newCurr;
			synchronized (currPos) {
				newCurr = currPos.addAndGet(pos);
				currPos.notifyAll();
			}
			buffer.rewind();
			buffer.limit(maxSize);

			LOG.debug("SYNC log to file. Size: " + pos + ", now: " + newCurr);
		}

		@Override
		public void close() throws IOException {
			sync();
			channel.close();
		}
	}

	private Config conf;
	private AtomicLong sequenceId;
	private String logFileName;
	private final AtomicInteger logCurrPos;

	private Map<Long, LogEntry> seqIdToEntry;

	private BlockingQueue<LogEntry> writeQueue;
	private BlockingQueue<LogEntry> readQueue;

	private WriteQueueConsumer writeQueueConsumer;
	private ReadQueueProducer readQueueProducer;

	public BaseLog(Config conf) throws IOException {
		this.conf = conf;
		this.sequenceId = new AtomicLong(0);
		this.logCurrPos = new AtomicInteger(0);
		this.logFileName = conf.getFullLogDir() + "/base.log";

		this.seqIdToEntry = new ConcurrentHashMap<>();

		this.writeQueue = new LinkedBlockingQueue<>();
		this.readQueue = new LinkedBlockingQueue<>();

		this.writeQueueConsumer = new WriteQueueConsumer();
		this.writeQueueConsumer.start();

		if (conf.getWalLevel() != WAL_LEVEL.SKIP && conf.getWalLevel() != WAL_LEVEL.ASYNC) {
			this.readQueueProducer = new ReadQueueProducer();
			this.readQueueProducer.start();
		}
	}

	public void notifyLogEntry(long seqId) {
		LogEntry entry = seqIdToEntry.get(seqId);
		seqIdToEntry.remove(seqId);
		entry.logNotify();
	}

	/**
	 * Concurrent
	 */
	private long write(WriteLogWal wal) {
		writeQueue.add(wal);
		wal.logJoin();
		return wal.getSeq();
	}

	@Override
	public BlockingQueue<LogEntry> getReadQueue() {
		return this.readQueue;
	}

	@Override
	public long put(byte[] key, byte[] value) {
		return write(WriteLogWal.createPutWal(key, value));
	}

	@Override
	public long delete(byte[] key) {
		return write(WriteLogWal.createDeleteWal(key));
	}

	@Override
	public void sync() {
		try {
			writeQueue.put(new LogSync());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		writeQueueConsumer.close();
	}
}
