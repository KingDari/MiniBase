package org.kingdari.MiniBase;

import java.io.IOException;

public class ReadLogWal implements LogEntry {

	private KeyValue kv;
	private boolean consumed;

	ReadLogWal(KeyValue kv) {
		this.kv = kv;
		this.consumed = false;
	}

	public int getSerializedSize() {
		return kv.getSerializedSize();
	}

	public byte[] toBytes() {
		return kv.toBytes();
	}

	public KeyValue getKv() {
		return kv;
	}

	public static ReadLogWal parseFrom(byte[] bytes, int offset) throws IOException {
		return new ReadLogWal(KeyValue.parseFrom(bytes, offset));
	}

	@Override
	public synchronized void logJoin() {
		while (!consumed) {
			try {
				this.wait();
			} catch (InterruptedException ignored) {
			}
		}
	}

	@Override
	public synchronized void logNotify() {
		this.consumed = true;
		this.notifyAll();
	}

	@Override
	public String toString() {
		return "LogWal{" + kv + '}';
	}
}
