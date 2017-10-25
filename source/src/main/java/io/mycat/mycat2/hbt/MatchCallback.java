package io.mycat.mycat2.hbt;

import java.util.List;
/**
 * join的時候a表的 aRow 與b表的bRow 那些字段需要傳輸到下一個管道
 * */
@FunctionalInterface
public interface MatchCallback {
	public abstract void call(List<byte[]> aRow, List<byte[]> bRow, List<byte[]> out);
}
