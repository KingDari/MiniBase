package org.kingdari.MiniBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.kingdari.MiniBase.DiskStore.MultiIter;
import org.kingdari.MiniBase.MStore.SeekIter;
import org.kingdari.MiniBase.Store.Iter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class MultiIterTest {
	public static class MockIter implements SeekIter<KeyValue> {
		private int cur;
		private KeyValue[] kvs;

		public MockIter(int[] array) throws IOException {
			assert array != null;
			this.kvs = new KeyValue[array.length];
			this.cur = 0;
			for (int i = 0; i < kvs.length; i++) {
				int v = array[i];
				this.kvs[i] = KeyValue.createPut(ByteUtils.toBytes(v),
								ByteUtils.toBytes(v), 1L);
			}
		}

		@Override
		public void seekTo(KeyValue kv) throws IOException {
			for (cur = 0; cur < kvs.length; cur++) {
				if (kvs[cur].compareTo(kv) >= 0) {
					break;
				}
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			return cur < kvs.length;
		}

		@Override
		public KeyValue next() throws IOException {
			return kvs[cur++];
		}

		@Override
		public void close() {
		}
	}

	@Test
	public void mergeSortTest() throws IOException {
		int iterNum = 10;
		int[][] arrays = new int[iterNum][];
		int[] sorted = new int[2000];
		SeekIter<KeyValue>[] iters =
						new SeekIter[iterNum];
		Random r = new Random();
		for (int i = 0; i < iterNum; i++) {
			arrays[i] = new int[200];
			for (int j = 0; j < arrays[i].length; j++) {
				int v = r.nextInt() % 10000;
				sorted[i * 200 + j] = v;
				arrays[i][j] = v;
			}
			Arrays.sort(arrays[i]);
			iters[i] = new MockIter(arrays[i]);
		}
		DiskStore.MultiIter multiIter = new DiskStore.MultiIter(iters);
		Arrays.sort(sorted);
		for (int i = 0; i < 2000; i++) {
			Assertions.assertEquals(
							sorted[i], ByteUtils.toInt(multiIter.next().getKey()));
		}
		Assertions.assertFalse(multiIter.hasNext());
	}

	@Test
	public void emptyTest() throws IOException {
		MockIter iter1 = new MockIter(new int[]{});
		MockIter iter2 = new MockIter(new int[]{});
		DiskStore.MultiIter multiIter = new DiskStore.MultiIter(new SeekIter[]{iter1, iter2});
		Assertions.assertFalse(multiIter.hasNext());
	}

	private void diskFileMergeSort(String[] inputs, String output, int rowCount)
			throws IOException {
		try {
			DiskFile.DiskFileWriter[] writers = new DiskFile.DiskFileWriter[inputs.length];
			DiskFile[] readers = new DiskFile[inputs.length];
			SeekIter<KeyValue>[] iters = new SeekIter[inputs.length];

			for (int i = 0; i < inputs.length; i++) {
				writers[i] = new DiskFile.DiskFileWriter(inputs[i]);
			}
			for (int i = 0; i < rowCount; i++) {
				int k = i % inputs.length;
				writers[k].append(KeyValue.createPut(
						ByteUtils.toBytes(i), ByteUtils.toBytes(i), 1L));
			}
			for (int i = 0; i < inputs.length; i++) {
				writers[i].appendIndex();
				writers[i].appendTrailer();
				writers[i].close();

				readers[i] = new DiskFile(inputs[i]).open();
				iters[i] = readers[i].iterator();
			}

			DiskFile.DiskFileWriter writer = new DiskFile.DiskFileWriter(output);
			try (MultiIter multiIter = new MultiIter(iters)) {
				while (multiIter.hasNext()) {
					writer.append(multiIter.next());
				}
			}

			writer.appendIndex();
			writer.appendTrailer();
			writer.close();

			for (DiskFile reader : readers) {
				reader.close();
			}

			DiskFile reader = new DiskFile(output).open();
			try (Iter<KeyValue> resultIter = reader.iterator()) {
				int count = 0;
				while (resultIter.hasNext()) {
					Assertions.assertEquals(resultIter.next(),
							KeyValue.createPut(ByteUtils.toBytes(count), ByteUtils.toBytes(count), 1L));
					count++;
				}
				Assertions.assertEquals(count, rowCount);
				Assertions.assertFalse(resultIter.hasNext());
			}
			reader.close();
		} finally {
			ArrayList<String> filesToDelete = new ArrayList<>(Arrays.asList(inputs));
			filesToDelete.add(output);
			for (String fileName : filesToDelete) {
				File f = new File(fileName);
				if (f.exists()) {
					f.delete();
				}
			}
		}
	}

	@Test
	public void diskFileMergeSortTest() throws IOException {
		diskFileMergeSort(new String[] { "a.db", "b.db" }, "c.db", 10);
		diskFileMergeSort(new String[] { "a.db" }, "b.db", 1);
		diskFileMergeSort(new String[] { "a.db", "b.db", "c.db" }, "d.db", 1000);
		diskFileMergeSort(new String[] { "a.db", "b.db", "c.db" }, "d.db", 100);
	}
}
