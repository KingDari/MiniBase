package org.kingdari.MiniBase;

import java.io.Closeable;
import java.io.IOException;

public interface MiniBase extends Closeable {
	void put(byte[] key, byte[] value) throws IOException;

	KeyValue get(byte[] key) throws IOException;

	void delete(byte[] key) throws IOException;

	Iter<KeyValue> scan(byte[] startKey, byte[] stopKey, KeyValueFilter filter) throws IOException;

	default Iter<KeyValue> scan(byte[] startKey, byte[] stopKey) throws IOException {
		return scan(startKey, stopKey, new KeyValueFilter());
	}

	default Iter<KeyValue> scan() throws IOException {
		return scan(ByteUtils.EMPTY_BYTES, ByteUtils.EMPTY_BYTES, new KeyValueFilter());
	}

	interface Iter<KeyValue> extends Closeable {
		boolean hasNext() throws IOException;

		KeyValue next() throws IOException;

		void close();
	}

	interface Flusher {
		void flush(Iter<KeyValue> iter) throws IOException;
	}

	abstract class Compactor extends Thread {
		public abstract void compact() throws IOException;

		public abstract void stopRunning();
	}
}
