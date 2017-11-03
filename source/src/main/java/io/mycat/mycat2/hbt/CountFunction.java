package io.mycat.mycat2.hbt;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
/**
 *  group 的时候进行count 某个字段
 * */
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
