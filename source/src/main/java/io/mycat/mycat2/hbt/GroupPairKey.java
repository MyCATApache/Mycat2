package io.mycat.mycat2.hbt;

import java.util.List;
/**
 * group by的 key1,key2
 * */
public class GroupPairKey {
	private int hash  = 0;
	private List<String> keyList;
	public GroupPairKey(List<String> keyList) {
		this.keyList = keyList;
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(obj instanceof GroupPairKey) {
			GroupPairKey another = (GroupPairKey)obj;
			if(another.keyList.size() == keyList.size()) {
				for(int i = 0 ; i < keyList.size(); i++) {
					String val1 = keyList.get(i);
					String val2 = another.keyList.get(i);
					
					if(val1 == null && val2 == null) {
						continue;
					}
					if(val1 != null && !val1.equals(val2)) {
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
