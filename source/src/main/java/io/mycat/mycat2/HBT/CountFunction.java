package io.mycat.mycat2.HBT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class CountFunction  implements Function<List<List<byte[]>>, List<byte[]>>{
	
	public List<byte[]> apply(List<List<byte[]>> list) {
		int n = 0;
		if(list != null) {
			n = 0;
		}
		n = list.size();
		String v = String.valueOf(n);
		ArrayList<byte[]> arrayList = new ArrayList<>();
		arrayList.add( v.getBytes());
		return arrayList;
	}
}
