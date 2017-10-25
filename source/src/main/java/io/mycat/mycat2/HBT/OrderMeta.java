package io.mycat.mycat2.HBT;

import java.util.List;

import javax.swing.SortOrder;

public class OrderMeta {

	private List<SortOrder> sortOrderList; 
	/**
	 * 一系列的key order by key1 asc, key2 desc
	 * */
	private List<String> columnsList;

	public OrderMeta(List<String> columnsList, List<SortOrder> sortOrderList) {
		this.columnsList = columnsList;
		this.sortOrderList = sortOrderList;
	}
	
	public List<SortOrder> getSortOrderList() {
		return sortOrderList;
	}

	public void setSortOrderList(List<SortOrder> sortOrderList) {
		this.sortOrderList = sortOrderList;
	}

	public List<String> getColumnsList() {
		return columnsList;
	}

	public void setColumnsList(List<String> columnsList) {
		this.columnsList = columnsList;
	}
	
}
