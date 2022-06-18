package org.kingdari.MiniBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class KeyValueTest {
	@Test
	public void compareTest() {
		KeyValue kv1 = KeyValue.createPut(
				ByteUtils.toBytes(100), ByteUtils.toBytes(20), 0L);
		Assertions.assertNotEquals(kv1, null);
		Assertions.assertNotEquals(kv1, new Object());
		Assertions.assertEquals(kv1, KeyValue.createPut(
				ByteUtils.toBytes(100), ByteUtils.toBytes(20), 0L));
		Assertions.assertNotEquals(kv1, KeyValue.createPut(
				ByteUtils.toBytes(1000), ByteUtils.toBytes(20), 0L));
		Assertions.assertNotEquals(kv1, KeyValue.createPut(
				ByteUtils.toBytes(100), ByteUtils.toBytes(200), 1L));
		Assertions.assertNotEquals(kv1, KeyValue.createDelete(
				ByteUtils.toBytes(100), 0L));

		KeyValue kv2 = KeyValue.createPut(
				ByteUtils.toBytes(-200), ByteUtils.toBytes(-200), 0L);
		KeyValue kv3 = KeyValue.createPut(
				ByteUtils.toBytes(400), ByteUtils.toBytes(200), 0L);
		KeyValue kv4 = KeyValue.createPut(
				ByteUtils.toBytes(100), ByteUtils.toBytes(-200), 0L);
		KeyValue kv5 = KeyValue.createPut(
				ByteUtils.toBytes(100), ByteUtils.toBytes(-200), 1L);
		Assertions.assertTrue(kv1.compareTo(kv2) > 0);
		Assertions.assertTrue(kv1.compareTo(kv3) < 0);
		Assertions.assertTrue(kv1.compareTo(kv4) == 0);
		Assertions.assertTrue(kv1.compareTo(kv5) > 0);
	}
}
