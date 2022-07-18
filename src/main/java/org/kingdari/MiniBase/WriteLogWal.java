package org.kingdari.MiniBase;

public class WriteLogWal implements LogEntry {

	public static WriteLogWal createPutWal(byte[] key, byte[] value) {
		return new WriteLogWal(key, value, KeyValue.Op.Put);
	}

	public static WriteLogWal createDeleteWal(byte[] key) {
		return new WriteLogWal(key, ByteUtils.EMPTY_BYTES, KeyValue.Op.Delete);
	}

	private KeyValue.Op op;
	private byte[] key;
	private byte[] value;
	private long seq;
	private KeyValue kv;
	private volatile boolean waiting;

	private WriteLogWal(byte[] key, byte[] value, KeyValue.Op op) {
		this.key = key;
		this.value = value;
		this.op = op;
		this.waiting = true;
	}

	public void setSeq(long seq) {
		this.seq = seq;
		if (op == KeyValue.Op.Put) {
			this.kv = KeyValue.createPut(key, value, seq);
		} else {
			this.kv = KeyValue.createDelete(key, seq);
		}
	}

	public long getSeq() {
		return seq;
	}

	public ReadLogWal toReadLogWal() {
		return new ReadLogWal(kv);
	}

	@Override
	public synchronized void logJoin() {
		while (waiting) {
			try {
				this.wait();
			} catch (InterruptedException ignored) {
			}
		}
	}

	@Override
	public synchronized void logNotify() {
		this.waiting = false;
		this.notifyAll();
	}

	@Override
	public int getSerializedSize() {
		return kv.getSerializedSize();
	}

	@Override
	public byte[] toBytes() {
		return kv.toBytes();
	}
}
