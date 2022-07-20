package org.kingdari.MiniBase;

import java.util.Objects;

public class BlockId {
	private DiskFile diskFile;
	private BlockMeta meta;

	public BlockId(DiskFile diskFile, BlockMeta meta) {
		this.diskFile = diskFile;
		this.meta = meta;
	}

	public DiskFile getDiskFile() {
		return diskFile;
	}

	public BlockMeta getMeta() {
		return meta;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BlockId blockId = (BlockId) o;
		return diskFile.equals(blockId.diskFile) && meta.equals(blockId.meta);
	}

	@Override
	public int hashCode() {
		return Objects.hash(diskFile, meta);
	}
}
