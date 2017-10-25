package io.mycat.mycat2.HBT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.SortOrder;

public class OrderPipeline extends ReferenceHBTPipeline {

	private OrderMeta orderMeta;
	private List<List<byte[]>> rowList;
	private ResultSetMeta resultSetMeta;
	public OrderPipeline(ReferenceHBTPipeline upStream, 
			OrderMeta orderMeta) {
		super(upStream);
		this.orderMeta = orderMeta;
		rowList = new ArrayList<>();
	}
	
	@Override
	public ResultSetMeta onHeader(ResultSetMeta header) {
		this.resultSetMeta = header;
		return super.onHeader(header);
	}
	
	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		rowList.add(row);
		return null;
	}
	
	@Override
	public void onEnd() {
		List<SortOrder> sortOrderList = orderMeta.getSortOrderList();
		List<String> columnsList = orderMeta.getColumnsList();
		int[] posList =	columnsList.stream().mapToInt(columns -> resultSetMeta.getFieldPos(columns)).toArray();
		List<OrderKey> orderKeyList = new ArrayList<>();
		for(int i = 0 ; i < rowList.size(); i++ ) {
			List<String> list = new ArrayList<>();
			List<byte[]> row = rowList.get(i);
			for(int pos : posList) {
				String val = new String(row.get(pos));
				list.add(val);
			}
			orderKeyList.add(new OrderKey(list, sortOrderList, i));
		}
		
		//排序 
		Collections.sort(orderKeyList);
		for(OrderKey orderKey : orderKeyList) {
			int index = orderKey.getIndex();
			super.onRowData(rowList.get(index));
		}
		super.onEnd();
	}

}
