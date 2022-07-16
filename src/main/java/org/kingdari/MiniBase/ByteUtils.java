package org.kingdari.MiniBase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ByteUtils {

	private static String HEX_TMP = "0123456789ABCDEF";

	public static final byte[] EMPTY_BYTES = new byte[0];

	public static String toHex(byte[] bytes) {
		return toHex(bytes, 0, bytes.length);
	}

	public static String toHex(byte[] bytes, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = offset; i < offset + len; i++) {
			int x = bytes[i];
			sb.append("\\x").
					append(HEX_TMP.charAt((x >> 4) & 0x0F)).
					append(HEX_TMP.charAt(x & 0x0F));
		}
		return sb.toString();
	}

	public static int hash(byte[] key) {
		if (key == null) {
			return 0;
		}
		int h = 1;
		for (byte b : key) {
			h = (h << 5) + h + b;
		}
		return h;
	}

	public static int compare(byte[] lhs, byte[] rhs) {
		if (lhs == rhs) {
			return 0;
		} else if (lhs == null) {
			return -1;
		} else if (rhs == null) {
			return 1;
		}
		for (int i = 0, j = 0; i < lhs.length && j < rhs.length; i++, j++) {
			int x = lhs[i] & 0xFF;
			int y = rhs[j] & 0xFF;
			if (x != y) {
				return x - y;
			}
		}
		return lhs.length - rhs.length;
	}

	public static byte[] toBytes(byte b) {
		return new byte[] {b};
	}

	public static byte[] toBytes(int v) {
		// Big-Endian
		byte[] bytes = new byte[4];
		for (int i = 3; i >= 0; i--) {
			int j = (3 - i) << 3;
			bytes[i] = (byte) ((v >> j) & 0xFF);
		}
		// reverse for comparing. positive > negative
		bytes[0] = (byte) (bytes[0] ^ 0x80);
		return bytes;
	}

	public static byte[] toBytes(long v) {
		byte[] bytes = new byte[8];
		for (int i = 7; i >= 0; i--) {
			int j = (7 - i) << 3;
			bytes[i] = (byte) ((v >> j) & 0xFF);
		}
		// reverse for comparing. positive > negative
		bytes[0] = (byte) (bytes[0] ^ 0x80);
		return bytes;
	}

	public static byte[] toBytes(String s) {
		return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
	}

	public static int toInt(byte[] bytes) {
		int v = 0;
		bytes[0] = (byte) (bytes[0] ^ 0x80);
		for (int i = 0; i < 4; i++) {
			int j = (3 - i) << 3;
			v |= ((0xFFL << j) & ((long) bytes[i] << j));
		}
		bytes[0] = (byte) (bytes[0] ^ 0x80);
		return v;
	}

	public static long toLong(byte[] bytes) {
		long v = 0;
		bytes[0] = (byte) (bytes[0] ^ 0x80);
		for (int i = 0; i < 8; i++) {
			int j = (7 - i) << 3;
			v |= ((0xFFL << j) & ((long) bytes[i] << j));
		}
		return v;
	}

	public static byte[] slice(byte[] bytes, int offset, int len) throws IOException {
		if (bytes == null) {
			throw new IOException("bytes is null");
		} else if (offset < 0 || len < 0 || offset + len > bytes.length) {
			throw new IOException("Invalid offset/len");
		}
		byte[] res = new byte[len];
		System.arraycopy(bytes, offset, res, 0, len);
		return res;
	}
}
