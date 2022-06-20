package org.kingdari.MiniBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.kingdari.MiniBase.DiskFile.DiskFileWriter;

import java.io.File;
import java.io.IOException;

public class KeyValueFilterTest {
	@Test
	public void bloomFilterTest() throws IOException {
		String fileName = "FilterTest";
		try {
			try (DiskFileWriter dfw = new DiskFileWriter(fileName)) {
				dfw.append(KeyValue.createPut(
						ByteUtils.toBytes(1), ByteUtils.toBytes(1), 1L));
				dfw.appendIndex();
				dfw.appendTrailer();
			}
			try (DiskFile df = new DiskFile(fileName).open()) {
				try (MStore.SeekIter<KeyValue> iter =
						     df.iterator(new KeyValueFilter().setKey(ByteUtils.toBytes(1)))) {
					Assertions.assertTrue(iter.hasNext());
					Assertions.assertEquals(iter.next(), KeyValue.createPut(
							ByteUtils.toBytes(1), ByteUtils.toBytes(1), 1L));
				}
				try (MStore.SeekIter<KeyValue> iter =
						     df.iterator(new KeyValueFilter().setKey(ByteUtils.toBytes(2)))) {
					Assertions.assertFalse(iter.hasNext());
				}
			}
		} finally {
			File f = new File(fileName);
			if (f.exists()) {
				f.delete();
			}
		}
	}
}
