package io.mycat.mycat2.HBT;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class PairKeyFunction implements Function<List<byte[]>,PairKey>{
	int[] indexList;

	public  PairKeyFunction(int[] indexList) {
		this.indexList = indexList;
	}
	
	@Override
	public PairKey apply(List<byte[]> rowList) {
		
		List<String> strList = new ArrayList<String>();
		
		for(int i : indexList) {
			String str = new String(rowList.get(i));
			strList.add(str);
		}
		
		PairKey pariKey = new PairKey(strList);
		return pariKey;
	}
	
}
