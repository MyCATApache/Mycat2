package io.mycat.mycat2.HBT;

import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import io.mycat.mysql.Fields;

public class JoinMeta extends Meta {
	public String lJoinKey;
	public String rJoinKey;
	public String lTable;
	public String rTable;
    private int limit;
	
	public JoinMeta(String lJoinKey, String rJoinKey, 
			String mem, int limit) {
		this.lJoinKey = parseField(lJoinKey);
		this.lTable = parseTable(lJoinKey);
		this.rTable = parseTable(rJoinKey);
		this.rJoinKey = parseField(rJoinKey);
		this.limit = limit;
	}
	
	public int getLimit() {
	    return limit;
	}
	
	private String parseTable(String joinKey) {
		int pos = joinKey.indexOf(".");
		if(pos <= 0) {
			return "";
		}
		return joinKey.substring(0, pos ); 
	}
	
	private String parseField(String joinKey) {
		int pos = joinKey.indexOf(".");
		if(pos <= 0) {
			return joinKey;
		}
		return joinKey.substring(pos + 1); 
	}

	public String getSql(List<String> joinKeyList, int fieldType, SqlMeta sqlMeta) {
		StringBuilder sb = new StringBuilder(sqlMeta.sql) ;
	    String values ;
		if(fieldType == Fields.FIELD_TYPE_STRING) {
		    values = joinKeyList.stream().collect(Collectors.joining("','","'","'"));
		} else {
	        values = joinKeyList.stream().collect(Collectors.joining(","));
		} 
		if(sb.indexOf("where") > 0) {
			sb.append(String.format(" and %s in (%s) ", rJoinKey, values));
		} else {
			sb.append(String.format(" where %s in (%s) " , rJoinKey, values));
		}
		return sb.toString();
	}


//	public RowMeta join(RowMeta aRowMeta, RowMeta bRowMeta, 
//			ResultSetMeta resultSetMeta,
//			MatchCallback<List<byte[]>> matchCallBack) {
		
//		RowMeta resultRowMeta = new RowMeta();
//		resultRowMeta.init(resultSetMeta);
//
//		Map<String, List<List<byte[]>>> keyMap = aRowMeta.getKeyMap(lJoinKey);
//		List<List<byte[]>> fieldValues = bRowMeta.getFieldValues();
//		Integer rjoinKeyPos = bRowMeta.getHeaderResultSet().getFieldPos(rJoinKey);
//		fieldValues.forEach(bRow -> {
//			String joinValue = new String(bRow.get(rjoinKeyPos));
//			List<List<byte[]>> lJoinList = keyMap.get(joinValue);
//			lJoinList.forEach(aRow -> {
//				matchCallBack.call(aRow, bRow, resultRowMeta);
//			});
//		});
//		return null;
//	}

//	public RowMeta joinResult(RowMeta aRowMeta, 
//		RowMeta bRowMeta, ResultSetMeta resutlSetMeta) {
//		List<String> fieldList = resutlSetMeta.getFieldList();
//		List<String> aFieldList = new ArrayList<String>();
//		List<String> bFieldList = new ArrayList<String>();
//
//		fieldList.forEach(key -> {
//			String table = parseTable(key);
//			String field = parseField(key);
//			Integer aPos = aRowMeta.getFiledPos(field);
//			Integer bPos = bRowMeta.getFiledPos(field);
//			if(!StringUtil.isEmpty(table)) {
//				if(table.equals(aRowMeta.table) ) {
//					aFieldList.add(field);
//				} else {
//					bFieldList.add(field);
//				}
//			} else {
//				if(aPos != null && bPos != null) 
//					throw new IllegalArgumentException(String.format("filed %s found in two table ", field));
//				if(aPos != null) aFieldList.add(field);
//				if(bPos != null) bFieldList.add(field);
//			}
//			
//		});
//		return plusResult(aRowMeta, aFieldList, bRowMeta, bFieldList);
//	}
//
//	private RowMeta plusResult(RowMeta aRowMeta, List<String> aFieldList, RowMeta bRowMeta,
//			List<String> bFieldList) {
//		RowMeta result = new RowMeta();
//		result.init(aFieldList.size() + bFieldList.size());
//		//plus header
//		
//		result.addHeader(aRowMeta, aFieldList);
//		result.addHeader(bRowMeta, bFieldList);
//		
//		Map<String, List<List<byte[]>>> filedNameMap = aRowMeta.getFiledNameMap(lJoinKey, aFieldList);
//		bRowMeta.join(filedNameMap, rJoinKey, bFieldList, result);
//		return result;
//	}
}
