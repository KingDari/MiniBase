package org.kingdari.MiniBase;

import java.io.Closeable;
import java.io.IOException;

public interface Region {
	void put(byte[] rowKey, byte[][] value) throws IOException;

	Row get(byte[] rowKey) throws IOException;

	void delete(byte[] rowKey) throws IOException;

	Iter<Row> scan(byte[] startRowKey, byte[] stopRowKey) throws IOException;

	default Iter<Row> scan() throws IOException {
		return scan(ByteUtils.EMPTY_BYTES, ByteUtils.EMPTY_BYTES);
	}

	interface Iter<Row> extends Closeable {
		boolean hasNext() throws IOException;

		Row next() throws IOException;

		void close();
	}
}
