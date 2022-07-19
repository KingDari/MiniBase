package org.kingdari.MiniBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kingdari.MiniBase.DiskFile.DiskFileWriter;

import java.io.File;
import java.io.IOException;

public class KeyValueFilterTest {

	private String fileName;

	@BeforeEach
	public void setUp() {
		this.fileName = "output/FilterTest";
		File f = new File(fileName);
		if (f.exists()) {
			f.delete();
		}
	}

	@AfterEach
	public void tearDown() {
		File f = new File(fileName);
		if (f.exists()) {
			f.delete();
		}
	}

	@Test
	public void bloomFilterTest() throws IOException {
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
	}

	@Test
	public void mvccFileTest() throws Exception {
		try (DiskFileWriter dfw = new DiskFileWriter(fileName)) {
			dfw.append(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(4), 5L));
			dfw.append(KeyValue.createDelete(
					ByteUtils.toBytes(1), 4L));
			dfw.append(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(3), 3L));
			dfw.append(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(2), 2L));
			dfw.append(KeyValue.createPut(
					ByteUtils.toBytes(1), ByteUtils.toBytes(1), 1L));
			dfw.appendIndex();
			dfw.appendTrailer();
		}
		try (DiskFile df = new DiskFile(fileName).open()) {
			try (MStore.SeekIter<KeyValue> iter =
					     df.iterator(new KeyValueFilter().setVersion(0L))) {
				Assertions.assertFalse(iter.hasNext());
			}
			try (MStore.SeekIter<KeyValue> iter =
							df.iterator(new KeyValueFilter().setVersion(1L))) {
				Assertions.assertTrue(iter.hasNext());
				Assertions.assertArrayEquals(ByteUtils.toBytes(1), iter.next().getValue());
				Assertions.assertFalse(iter.hasNext());
			}
			try (MStore.SeekIter<KeyValue> iter =
					     df.iterator(new KeyValueFilter().setVersion(2L))) {
				Assertions.assertTrue(iter.hasNext());
				Assertions.assertArrayEquals(ByteUtils.toBytes(2), iter.next().getValue());
				Assertions.assertArrayEquals(ByteUtils.toBytes(1), iter.next().getValue());
				Assertions.assertFalse(iter.hasNext());
			}
			try (MStore.SeekIter<KeyValue> iter =
					     df.iterator(new KeyValueFilter().setVersion(4L))) {
				Assertions.assertTrue(iter.hasNext());
				Assertions.assertEquals(KeyValue.Op.Delete, iter.next().getOp());
			}
			try (MStore.SeekIter<KeyValue> iter =
					     df.iterator(new KeyValueFilter().setVersion(5L))) {
				Assertions.assertTrue(iter.hasNext());
				Assertions.assertArrayEquals(ByteUtils.toBytes(4), iter.next().getValue());
			}
			try (MStore.SeekIter<KeyValue> iter =
					     df.iterator(new KeyValueFilter().setVersion(1000L))) {
				Assertions.assertTrue(iter.hasNext());
				Assertions.assertArrayEquals(ByteUtils.toBytes(4), iter.next().getValue());
			}
		}
	}
}
