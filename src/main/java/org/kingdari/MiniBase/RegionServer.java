package org.kingdari.MiniBase;

import org.kingdari.MiniBase.Region.Iter;
import java.io.IOException;

public interface RegionServer {
	void put(byte[] region, byte[] rowKey, byte[][] value) throws IOException;

	Row get(byte[] region, byte[] rowKey) throws IOException;

	void delete(byte[] region, byte[] rowKey) throws IOException;

	Iter<Row> scan(byte[] region, byte[] startRowKey, byte[] stopRowKey) throws IOException;

	default Iter<Row> scan(byte[] region) throws IOException {
		return scan(region, ByteUtils.EMPTY_BYTES, ByteUtils.EMPTY_BYTES);
	}
}
