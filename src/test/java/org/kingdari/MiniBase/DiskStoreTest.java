package org.kingdari.MiniBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DiskStoreTest {
	@Test
	public void basicTest() throws IOException, InterruptedException {
		DiskStore ds = new DiskStore("basicTest", 20);
		ds.open();
		ds.close();
	}
}
