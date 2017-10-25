package io.mycat.mycat2.HBT;

import java.util.List;
/**
 * group by的 key1,key2
 * */
public class PairKey {
	private int hash  = 0;
	private List<String> keyList;
	public PairKey(List<String> keyList) {
		this.keyList = keyList;
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(obj instanceof PairKey) {
			PairKey another = (PairKey)obj;
			if(another.keyList.size() == keyList.size()) {
				for(int i = 0 ; i < keyList.size(); i++) {
					if(!keyList.get(i).equals(another.keyList.get(i))) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}
	@Override
	public int hashCode() {
		int h = hash;
        if (h == 0 && keyList.size() > 0) {

            for (int i = 0; i < keyList.size(); i++) {
                h = 31 * h + keyList.hashCode();
            }
            hash = h;
        }
        return h;
		
	}
}
