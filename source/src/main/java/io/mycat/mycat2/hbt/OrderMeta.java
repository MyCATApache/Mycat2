package io.mycat.mycat2.hbt;

import java.util.List;

import javax.swing.SortOrder;

public class OrderMeta {
	/**
	 * 記錄某個字段是升序還是降序
	 * */
	private List<SortOrder> sortOrderList; 
	/**
	 * 一系列的key order by key1 asc, key2 desc
	 * 字段名稱
	 */
	private List<String> fieldNameList;

	public OrderMeta(List<String> fieldNameList, List<SortOrder> sortOrderList) {
		this.fieldNameList = fieldNameList;
		this.sortOrderList = sortOrderList;
	}
	
	public List<SortOrder> getSortOrderList() {
		return sortOrderList;
	}

	public void setSortOrderList(List<SortOrder> sortOrderList) {
		this.sortOrderList = sortOrderList;
	}

	public List<String> getColumnsList() {
		return fieldNameList;
	}

	public void setColumnsList(List<String> columnsList) {
		this.fieldNameList = columnsList;
	}
	
}
