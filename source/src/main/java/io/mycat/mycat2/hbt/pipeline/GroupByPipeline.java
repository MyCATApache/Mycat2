package io.mycat.mycat2.hbt.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.mycat.mycat2.hbt.GroupPairKey;
import io.mycat.mycat2.hbt.GroupPairKeyMeta;
import io.mycat.mycat2.hbt.ResultSetMeta;

public class GroupByPipeline extends ReferenceHBTPipeline {
	
	List<Function< List<List<byte[]>>, List<byte[]> >> opFunction = new ArrayList<>();
	GroupPairKeyMeta keyFunction;
	Map<GroupPairKey, List<List<byte[]>>> groupMap;
	private ResultSetMeta resultSetMeta;
	public GroupByPipeline(ReferenceHBTPipeline upStream, 
			GroupPairKeyMeta keyFunction , ResultSetMeta resultSetMeta , List<Function<List<List<byte[]>>,List<byte[]>>> opFunction) {
		super(upStream);
		this.opFunction = opFunction;
		this.keyFunction = keyFunction;
		groupMap = new HashMap<>();
		this.resultSetMeta = resultSetMeta;
	}
	
	@Override
	public ResultSetMeta onHeader(ResultSetMeta header) {
		
		keyFunction.init(header);
		
		
		return super.onHeader(resultSetMeta);
	}
	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		/* 获取key */
		GroupPairKey pairKey = keyFunction.apply(row);
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
