package org.kingdari.MiniBase;

public class Config {
	private long maxMemStoreSize = 16 * 1024 * 1024;
	private int flushMaxRetryTimes = 10;
	private String dataDir = "MiniBase";
	private int maxDiskFiles = 10;
	private int maxThreadPoolSize = 5;

	private static final Config DEFAULT = new Config();

	public static Config getDefault() {
		return DEFAULT;
	}

	public Config setFlushMaxRetryTimes(int flushMaxRetryTimes) {
		this.flushMaxRetryTimes = flushMaxRetryTimes;
		return this;
	}

	public Config setMaxMemStoreSize(long maxMemStoreSize) {
		this.maxMemStoreSize = maxMemStoreSize;
		return this;
	}

	public Config setDataDir(String dataDir) {
		this.dataDir = dataDir;
		return this;
	}

	public Config setMaxDiskFiles(int maxDiskFiles) {
		this.maxDiskFiles = maxDiskFiles;
		return this;
	}

	public Config setMaxThreadPoolSize(int maxThreadPoolSize) {
		this.maxThreadPoolSize = maxThreadPoolSize;
		return this;
	}

	public Config setMaxMemStoreSize(int maxMemStoreSize) {
		this.maxMemStoreSize = maxMemStoreSize;
		return this;
	}

	public int getFlushMaxRetryTimes() {
		return flushMaxRetryTimes;
	}

	public int getMaxThreadPoolSize() {
		return maxThreadPoolSize;
	}

	public long getMaxMemStoreSize() {
		return maxMemStoreSize;
	}

	public int getMaxDiskFiles() {
		return maxDiskFiles;
	}

	public String getDataDir() {
		return dataDir;
	}

}
