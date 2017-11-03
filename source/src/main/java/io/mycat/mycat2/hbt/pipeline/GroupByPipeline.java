package io.mycat.mycat2.hbt.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.mycat.mycat2.hbt.GroupPairKey;
import io.mycat.mycat2.hbt.GroupPairKeyMeta;
import io.mycat.mycat2.hbt.ResultSetMeta;


/**
 * 进行group操作
 * 
 * @author zhangwy
 * */
public class GroupByPipeline extends ReferenceHBTPipeline {
	/*
	 * 对于group之后的数据进行的一系列操作 例如count 或者输出某个字段
	 * */
	List<Function< List<List<byte[]>>, List<byte[]> >> opFunction;
	/* group的字段*/
	GroupPairKeyMeta keyFunction;
	
	/*分组的函数 key为group的字段, value为所有key相同的row*/
	Map<GroupPairKey, List<List<byte[]>>> groupMap;
	/*字段的头信息*/
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
		/*整理list 进行group分组*/
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
			/*
			 * 对group完的List 分别执行count 或者avg 或者直接输出某几列
			 * */
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
		
		/**
		 * 管道执行下一个函数
		 * */
		for(List<byte[]> rowList : rowLists ) {
			super.onRowData(rowList);
		}
		
		super.onEnd();
	}
}
