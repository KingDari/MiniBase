package org.kingdari.MiniBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class ByteUtilsTest {
	@Test
	public void toBytesTest() {
		Assertions.assertArrayEquals(ByteUtils.toBytes((byte) 'a'), new byte[]{'a'});
	}

	@Test
	public void intTest() {
		Assertions.assertEquals(ByteUtils.toInt(ByteUtils.toBytes(123456789)), 123456789);
		Assertions.assertEquals(ByteUtils.toInt(ByteUtils.toBytes(-1)), -1);
		Assertions.assertEquals(ByteUtils.toInt(ByteUtils.toBytes(0)), 0);
		Assertions.assertEquals(ByteUtils.toInt(ByteUtils.toBytes(1)), 1);
		Assertions.assertEquals(ByteUtils.toInt(ByteUtils.toBytes(Integer.MAX_VALUE)), Integer.MAX_VALUE);
		Assertions.assertEquals(ByteUtils.toInt(ByteUtils.toBytes(Integer.MIN_VALUE)), Integer.MIN_VALUE);
	}

	@Test
	public void longTest() {
		Assertions.assertEquals(ByteUtils.toLong(ByteUtils.toBytes(123456789L)), 123456789L);
		Assertions.assertEquals(ByteUtils.toLong(ByteUtils.toBytes(-1L)), -1L);
		Assertions.assertEquals(ByteUtils.toLong(ByteUtils.toBytes(0L)), 0L);
		Assertions.assertEquals(ByteUtils.toLong(ByteUtils.toBytes(1L)), 1L);
		Assertions.assertEquals(ByteUtils.toLong(ByteUtils.toBytes(Long.MAX_VALUE)), Long.MAX_VALUE);
		Assertions.assertEquals(ByteUtils.toLong(ByteUtils.toBytes(Long.MIN_VALUE)), Long.MIN_VALUE);
	}

	@Test
	public void toHexTest() {
		byte[] bytes = ByteUtils.toBytes(123456);
		Assertions.assertEquals(ByteUtils.toHex(bytes), "\\x80\\x01\\xE2\\x40");
		Assertions.assertEquals(ByteUtils.toHex(bytes, 2, 1), "\\xE2");
	}

	@Test
	public void compareTest() {
		Assertions.assertEquals(ByteUtils.compare(null, null), 0);
		Assertions.assertEquals(ByteUtils.compare(null, new byte[0]), -1);
		Assertions.assertEquals(ByteUtils.compare(new byte[] {1}, new byte[0]), 1);
		Assertions.assertEquals(ByteUtils.compare(new byte[] {1}, new byte[] {1}), 0);
		Assertions.assertEquals(ByteUtils.compare(new byte[] {1, 2, 3}, new byte[] {1, 3, 2}), -1);
		Assertions.assertEquals(ByteUtils.compare(
				new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
				new byte[] {(byte) 0xFF, (byte) 0xFF}), 1);
	}
}
