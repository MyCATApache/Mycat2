package io.mycat.mycat2.HBT;

@FunctionalInterface
public interface MatchCallback<T> {
	public abstract void call(T aRow, T bRow, RowMeta out);
}
