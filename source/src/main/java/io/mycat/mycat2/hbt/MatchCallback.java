package io.mycat.mycat2.hbt;

import java.util.List;

@FunctionalInterface
public interface MatchCallback {
	public abstract void call(List<byte[]> aRow, List<byte[]> bRow, List<byte[]> out);
}
