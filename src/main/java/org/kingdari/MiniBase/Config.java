package org.kingdari.MiniBase;

public class Config {

	public enum WAL_LEVEL {
		SKIP,
		ASYNC,
		SYNC,
		FSYNC
	}

	private int logBufferSize = 16 * 1024 * 1024;
	private long maxMemStoreSize = 16 * 1024 * 1024;
	private int flushMaxRetryTimes = 10;
	private int putMaxRetryTimes = 100;
	private String rootDir = "MiniBase";
	private String dataDir = "Data";
	private String logDir = "Log";
	private int maxDiskFiles = 10;
	private int maxThreadPoolSize = 5;
	private WAL_LEVEL walLevel = WAL_LEVEL.SKIP;

	private static final Config DEFAULT = new Config();

	public static Config getDefault() {
		return DEFAULT;
	}

	public Config setPutMaxRetryTimes(int times) {
		this.putMaxRetryTimes = times;
		return this;
	}

	public Config setLogBufferSize(int size) {
		this.logBufferSize = size;
		return this;
	}

	public Config setRoorDir(String rootDir) {
		this.rootDir = rootDir;
		return this;
	}

	public Config setLogDir(String logDir) {
		this.logDir = logDir;
		return this;
	}

	public Config setWalLevel(WAL_LEVEL walLevel) {
		this.walLevel = walLevel;
		return this;
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

	public int getPutMaxRetryTimes() {
		return putMaxRetryTimes;
	}

	public int getLogBufferSize() {
		return logBufferSize;
	}

	public String getFullLogDir() {
		return rootDir + "/" + logDir;
	}

	public String getFullDataDir() {
		return rootDir + "/" + dataDir;
	}

	public String getRootDir() {
		return rootDir;
	}

	public String getLogDir() {
		return logDir;
	}

	public WAL_LEVEL getWalLevel() {
		return walLevel;
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
