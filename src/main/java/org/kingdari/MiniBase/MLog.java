package org.kingdari.MiniBase;

import java.io.Closeable;

public interface MLog extends Closeable {

	long put(byte[] key, byte[] value);

	long delete(byte[] key);

	void sync();
}
