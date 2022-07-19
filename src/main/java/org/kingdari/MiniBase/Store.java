package org.kingdari.MiniBase;

import java.io.Closeable;
import java.io.IOException;

public interface Store extends Closeable {
	void put(byte[] key, byte[] value) throws IOException;

	void delete(byte[] key) throws IOException;

	KeyValue get(KeyValueFilter filter) throws IOException;

	Iter<KeyValue> scan(KeyValueFilter filter) throws IOException;

	default Iter<KeyValue> scan() throws IOException {
		return scan(KeyValueFilter.createEmptyFilter());
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
