package org.kingdari.MiniBase;

import java.io.IOException;

public class KeyValue implements Comparable<KeyValue> {

	public static final int RAW_KEY_LEN_SIZE = 4;
	public static final int VAL_LEN_SIZE = 4;
	public static final int OP_SIZE = 1;
	public static final int SEQ_ID_SIZE = 8;

	private byte[] key;
	private byte[] value;
	private Op op;
	private long sequenceId;

	public enum Op {
		Put((byte) 0),
		Delete((byte) 1);

		private byte code;

		Op(byte code) {
			this.code = code;
		}

		public static Op codeToOp(byte code) {
			switch (code) {
				case 0:
					return Put;
				case 1:
					return Delete;
				default:
					throw new IllegalArgumentException("unknown code: " + code);
			}
		}

		public byte getCode() {
			return this.code;
		}
	}

	public static KeyValue createPut(byte[] key, byte[] value, long sequenceId) {
		return new KeyValue(key, value, Op.Put, sequenceId);
	}

	public static KeyValue createDelete(byte[] key, long sequenceId) {
		return new KeyValue(key, ByteUtils.EMPTY_BYTES, Op.Delete, sequenceId);
	}

	private KeyValue(byte[] key, byte[] value, Op op, long sequenceId) {
		assert key != null;
		assert value != null;
		assert op != null;
		assert sequenceId >= -1;
		this.key = key;
		this.value = value;
		this.op = op;
		this.sequenceId = sequenceId;
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getValue() {
		return value;
	}

	public Op getOp() {
		return op;
	}

	public long getSequenceId() {
		return sequenceId;
	}

	private int getRawKeyLen() {
		return key.length + OP_SIZE + SEQ_ID_SIZE;
	}

	public int getSerializedSize() {
		return RAW_KEY_LEN_SIZE + VAL_LEN_SIZE + getRawKeyLen() + value.length;
	}

	public byte[] toBytes() {
		int rawKeyLen = getRawKeyLen();
		int pos = 0;
		byte[] bytes = new byte[getSerializedSize()];

		// rawKeyLen
		byte[] rawKeyLenBytes = ByteUtils.toBytes(rawKeyLen);
		System.arraycopy(rawKeyLenBytes, 0, bytes, pos, RAW_KEY_LEN_SIZE);
		pos += RAW_KEY_LEN_SIZE;

		byte[] valLen = ByteUtils.toBytes(value.length);
		System.arraycopy(valLen, 0, bytes, pos, VAL_LEN_SIZE);
		pos += VAL_LEN_SIZE;

		System.arraycopy(key, 0, bytes, pos, key.length);
		pos += key.length;

		bytes[pos++] = op.getCode();

		byte[] seqIdBytes = ByteUtils.toBytes(sequenceId);
		System.arraycopy(seqIdBytes, 0, bytes, pos, seqIdBytes.length);
		pos += seqIdBytes.length;

		System.arraycopy(value, 0, bytes, pos, value.length);
		pos += value.length;

		assert pos == getSerializedSize();
		return bytes;
	}

	public static KeyValue parseFrom(byte[] bytes, int offset) throws IOException {
		if (bytes == null) {
			throw new IOException("buffer is null");
		} else if (offset + RAW_KEY_LEN_SIZE + VAL_LEN_SIZE >= bytes.length) {
			throw new IOException("Invalid len");
		}
		int pos = offset;
		int rawKeyLen = ByteUtils.toInt(ByteUtils.slice(bytes, pos, RAW_KEY_LEN_SIZE));
		pos += RAW_KEY_LEN_SIZE;
		int valLen = ByteUtils.toInt(ByteUtils.slice(bytes, pos, VAL_LEN_SIZE));
		pos += VAL_LEN_SIZE;
		int keyLen = rawKeyLen - OP_SIZE - SEQ_ID_SIZE;
		byte[] key = ByteUtils.slice(bytes, pos, keyLen);
		pos += keyLen;
		Op op = Op.codeToOp(bytes[pos]);
		pos += OP_SIZE;
		long sequenceId = ByteUtils.toLong(ByteUtils.slice(bytes, pos, SEQ_ID_SIZE));
		pos += SEQ_ID_SIZE;
		byte[] val = ByteUtils.slice(bytes, pos, valLen);
		pos += valLen;

		assert pos <= bytes.length;
		return new KeyValue(key, val, op, sequenceId);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object kv) {
		if (kv == null) return false;
		if (!(kv instanceof KeyValue)) return false;
		KeyValue that = (KeyValue) kv;
		return this.compareTo(that) == 0;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("key=").append(ByteUtils.toHex(this.key)).append("/op=").
				append(op).append("/sequenceId=").append(this.sequenceId).
				append("/value=").append(ByteUtils.toHex(this.value));
		return sb.toString();
	}

	// sort from negative to positive
	@Override
	public int compareTo(KeyValue kv) {
		if (kv == null) {
			throw new IllegalArgumentException("kv must not be null");
		}
		int ret = ByteUtils.compare(this.key, kv.key);
		if (ret != 0) {
			return ret;
		} else if (this.sequenceId != kv.sequenceId) {
			return this.sequenceId > kv.sequenceId ? -1 : 1;
		} else if (this.op != kv.op) {
			return this.op.getCode() > kv.op.getCode() ? -1 : 1;
		} else {
			return 0;
		}
	}
}
