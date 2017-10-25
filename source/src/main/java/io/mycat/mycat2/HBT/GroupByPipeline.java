package io.mycat.mycat2.HBT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupByPipeline extends ReferenceHBTPipeline {
	
	List<Function< List<List<byte[]>>, List<byte[]> >> opFunction = new ArrayList<>();
	Function<List<byte[]>,PairKey> keyFunction;
	Map<PairKey, List<List<byte[]>>> groupMap;
	private ResultSetMeta resultSetMeta;
	public GroupByPipeline(ReferenceHBTPipeline upStream, 
			Function<List<byte[]>,PairKey> keyFunction , ResultSetMeta resultSetMeta , List<Function<List<List<byte[]>>,List<byte[]>>> opFunction) {
		super(upStream);
		this.opFunction = opFunction;
		this.keyFunction = keyFunction;
		groupMap = new HashMap<>();
		this.resultSetMeta = resultSetMeta;
	}
	
	@Override
	public ResultSetMeta onHeader(ResultSetMeta header) {
		
		return super.onHeader(resultSetMeta);
	}
	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		/* 获取key */
		PairKey pairKey = keyFunction.apply(row);
		/*整理list*/
		List<List<byte[]>> rowList = groupMap.get(pairKey);
		if(rowList == null) {
			rowList = new ArrayList<>();
			groupMap.put(pairKey, rowList);
		}
		rowList.add(row);
		return null;
	}
	
	@Override
	public void onEnd() {
		List<List<byte[]>> rowLists = groupMap.values().stream().map(rowList -> {
			List<byte[]> resultList = null;
			for(Function<List<List<byte[]>>, List<byte[]>> func : opFunction) {
				List<byte[]> result = func.apply(rowList);
				if(null == resultList) {
					resultList = result;
				} else {
					resultList.addAll(result);
				}
			}
			return resultList;
		}).collect(Collectors.toList());
		
		for(List<byte[]> rowList : rowLists ) {
			super.onRowData(rowList);
		}
		
		super.onEnd();
	}
}
