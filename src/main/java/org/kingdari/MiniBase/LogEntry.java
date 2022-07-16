package org.kingdari.MiniBase;

public interface LogEntry {
	void logJoin();

	void logNotify();

	int getSerializedSize();

	byte[] toBytes();
}
