package org.kingdari.MiniBase;

import org.kingdari.MiniBase.MStore.SeekIter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class BlockReader {

	static class DataBlockIterator implements SeekIter<KeyValue> {
		private BlockReader reader;
		private KeyValueFilter filter;
		private List<KeyValue> list;
		private int pos;

		public DataBlockIterator(BlockReader reader, KeyValueFilter filter) {
			this.reader = reader;
			this.filter = filter;
			this.list = reader.getKeyValues();
			this.pos = 0;
		}

		public KeyValue fetchNext() throws IOException {
			while (pos < list.size()) {
				if (filter.isVisible(list.get(pos))) {
					return list.get(pos);
				} else {
					pos++;
				}
			}
			return null;
		}

		@Override
		public void seekTo(KeyValue target) throws IOException {
			int l = 0, r = list.size() - 1;
			while (l <= r) {
				int m = (r - l) / 2 + l;
				KeyValue kv = list.get(m);
				if (filter.isVisible(kv) && kv.compareTo(target) >= 0) {
					r = m - 1;
				} else {
					l = m + 1;
				}
			}
			pos = l;
		}

		@Override
		public boolean hasNext() throws IOException {
			KeyValue kv = fetchNext();
			return kv != null;
		}

		@Override
		public KeyValue next() throws IOException {
			KeyValue kv = fetchNext();
			pos++;
			return kv;
		}

		@Override
		public void close() {

		}
	}

	private List<KeyValue> kvBuf;
	private long memorySize;

	BlockReader(List<KeyValue> kvBuf) {
		this.kvBuf = kvBuf;
		this.memorySize = 0L;
		for (KeyValue kv : kvBuf) {
			this.memorySize += kv.getMemorySize();
		}
	}

	public static BlockReader parseFrom(byte[] buffer, int offset, int size) throws IOException {
		int pos = offset;
		List<KeyValue> kvBuf = new ArrayList<>();
		Checksum crc32 = new CRC32();

		int kvSize = ByteUtils.toInt(ByteUtils.slice(buffer, pos, BlockWriter.KV_SIZE_LEN));
		pos += BlockWriter.KV_SIZE_LEN;

		for (int i = 0; i < kvSize; i++) {
			KeyValue kv = KeyValue.parseFrom(buffer, pos);
			kvBuf.add(kv);
			crc32.update(buffer, pos, kv.getSerializedSize());
			pos += kv.getSerializedSize();
		}

		int checksum = ByteUtils.toInt(ByteUtils.slice(buffer, pos, BlockWriter.CHECKSUM_LEN));
		pos += BlockWriter.CHECKSUM_LEN;

		assert checksum == (int) crc32.getValue();
		assert pos == size : "pos: " + pos + ", size: " + size;
		return new BlockReader(kvBuf);
	}

	public long getMemorySizeSize() {
		return memorySize;
	}

	public List<KeyValue> getKeyValues() {
		return kvBuf;
	}

	public DataBlockIterator iterator(KeyValueFilter filter) {
		return new DataBlockIterator(this, filter);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BlockReader that = (BlockReader) o;
		return Objects.equals(kvBuf, that.kvBuf);
	}

	@Override
	public int hashCode() {
		return Objects.hash(kvBuf);
	}
}

