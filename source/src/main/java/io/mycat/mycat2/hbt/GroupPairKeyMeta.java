package io.mycat.mycat2.hbt;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.mycat.util.StringUtil;

/**
 *  group 的时候进行group的字段的编号
 * */


public class GroupPairKeyMeta implements Function<List<byte[]>,GroupPairKey>{
	/*所有需要group的字段的序号,从零开始*/
	private String[] columnsList;
	private int[] indexList;
	public  GroupPairKeyMeta(String... columnsList) {
		this.columnsList = columnsList;
	}
	public String[] getColumnsList() {
		return columnsList;
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
	 * 通过column 的名称找到对应的列的名字 
	 * */
	public  void init(ResultSetMeta header) {
		indexList = new int[columnsList.length];
		for(int index = 0 ; index < columnsList.length; index ++) {
			System.out.println(columnsList[index]);
			indexList[index] = header.getFieldPos(columnsList[index]);
		}
	}
}
