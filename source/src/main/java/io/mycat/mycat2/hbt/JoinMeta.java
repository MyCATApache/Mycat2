package io.mycat.mycat2.hbt;

import java.util.List;
import java.util.stream.Collectors;

import io.mycat.mysql.Fields;

/**
 * ltable表的  lJoinKey 與 b表 的 rJoinKey進行關聯
 *
 * */
public class JoinMeta extends Meta {
	public String lJoinKey;
	public String rJoinKey;
	public String lTable;

	/*  rTable b表*/
	public String rTable;
	/*  limit 多少個id發起一個後端請求*/
    private int limit; 
	/**
	 * @param lJoinKey a表關聯所對應的字段
	 * @param rJoinKey b表關聯所對應的字段
	 * @param mem 
	 * @param limit 多少個id發起一個後端請求
	 * */
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
	
	/**
	 *  joinKeyList 一系列的ljoinKey的值
	 *  
	 *  @return 組成sql進行返回 
	 *   	    e.g select key1,key2 from table where xx = 'xx' and rJoinKey in ('id1','id2') 
	 * */
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



}
