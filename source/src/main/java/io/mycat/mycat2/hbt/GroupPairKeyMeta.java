package io.mycat.mycat2.hbt;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.mycat.util.StringUtil;

/**
 *  group 的时候进行group的字段的编号
 * */


public class GroupPairKeyMeta implements Function<List<byte[]>,GroupPairKey>{
	/*所有需要group的字段的名稱,从零开始*/
	private String[] fieldNameList;
	/*所有需要group的字段的序号,从零开始*/
	private int[] indexList;
	public  GroupPairKeyMeta(String... fieldNameList) {
		this.fieldNameList = fieldNameList;
	}
	public String[] getFieldNameList() {
		return fieldNameList;
	}
	public int[] getIndexList() {
		return indexList;
	}
	public void setIndexList(int[] indexList) {
		this.indexList = indexList;
	}
	
	
	/**
	 * 每一行中获取需要
	 * */
	@Override
	public GroupPairKey apply(List<byte[]> rowList) {
		
		List<String> strList = new ArrayList<String>();
		
		for(int i : indexList) {
			String str = StringUtil.parseString(rowList.get(i)) ;
			strList.add(str);
		}
		
		GroupPairKey pariKey = new GroupPairKey(strList);
		return pariKey;
	}
	/**
	 * 通过column 的名称找到对应的列的名字所對應的位置 
	 * */
	public  void init(ResultSetMeta header) {
		indexList = new int[fieldNameList.length];
		for(int index = 0 ; index < fieldNameList.length; index ++) {
			System.out.println(fieldNameList[index]);
			indexList[index] = header.getFieldPos(fieldNameList[index]);
		}
	}
}
