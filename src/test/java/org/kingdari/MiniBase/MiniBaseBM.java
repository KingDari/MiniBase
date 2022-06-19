package org.kingdari.MiniBase;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.All)
@Warmup(iterations = 0, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(5)
@Fork(1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MiniBaseBM {

	private static final Logger LOG = LogManager.getLogger(MiniBaseBM.class);

	@Param(value = {"4", "8", "16"})
	private long memStoreSize;
	private int retryMaxTimes = 10;

	private MiniBase db;
	private String dataDir;

	@Setup
	public void initDB() {
		dataDir = "output/MiniBase-" + System.currentTimeMillis();
		File f = new File(dataDir);
		Assertions.assertTrue(f.mkdirs());
		Config conf = new Config().
				setDataDir(dataDir).
				setMaxMemStoreSize(memStoreSize * 1024 * 1024).
				setFlushMaxRetryTimes(3).
				setMaxDiskFiles(10);
		try {
			this.db = MStore.create(conf).open();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@TearDown
	public void finalize() throws IOException {
		db.close();
		File f = new File(dataDir);
		for (File file : f.listFiles()) {
			file.delete();
		}
		f.delete();
	}

	@Benchmark
	public void measurePut(Blackhole bh) throws InterruptedException {
		for (int i = 0; i < retryMaxTimes; i++) {
			try {
				db.put(ByteUtils.toBytes(i), ByteUtils.toBytes(i));
				break;
			} catch (IOException e) {
				Thread.sleep(100L << i);
			}
		}
		assert false;
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(MiniBaseBM.class.getSimpleName())
				.result("result.json")
				.resultFormat(ResultFormatType.JSON)
				.build();
		new Runner(opt).run();
	}
}
