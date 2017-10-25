package io.mycat.mycat2.hbt;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class OutFunction implements Function<List<List<byte[]>>, List<byte[]>>{
	private int[] indexList;
	
	public OutFunction(int... indexList) {
		this.indexList = indexList;
	}
	
	@Override
	public List<byte[]> apply(List<List<byte[]>> list) {
		ArrayList<byte[]> rowList = new ArrayList<>();
		if(indexList != null) {
			for(int i : indexList) {
				rowList.add(list.get(0).get(i));
			}
			return rowList;
		} 
		return rowList;
	}
}
