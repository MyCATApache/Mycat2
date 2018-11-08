package io.mycat.mycat2.hbt;

import io.mycat.mysql.packet.FieldPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.PacketUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 字段以及字段所有對應的類型
 * */
public class ResultSetMeta extends Meta {
	/*所有的字段名稱*/
	private List<String> fieldNameList;
	/*字段所對應的位置*/
	private Map<String,Integer> fieldPosMap;
	/*字段的類型*/
	private int[] fieldTypeList;
	/*字段的數量*/
	private int filedCount ;
	
	public ResultSetMeta(List<String> fieldNameList, int[] fieldTypeList) {
		this.fieldNameList = fieldNameList;
		this.fieldTypeList = fieldTypeList;
		this.filedCount = fieldNameList.size();
		this.fieldPosMap = new HashMap<String,Integer>();

		for (int i = 0; i < fieldNameList.size(); i++) {
			fieldPosMap.put(fieldNameList.get(i), i);
		}
	}


	public void addField(String fieldName, int fieldType) {
		int pos = fieldNameList.size(); 
		fieldNameList.add(fieldName);
		fieldPosMap.put(fieldName, pos);
		fieldTypeList[pos] = fieldType;
	}
	

	
	public ResultSetMeta(int filedCount) {
		this.fieldNameList = new ArrayList<String>();
		this.filedCount = filedCount;
		this.fieldTypeList = new int[filedCount];
		fieldPosMap = new HashMap<String,Integer>();

	}

	public int getRealFieldNameListSize() {
	    return this.fieldNameList.size();
	}
	
	public List<String> getFieldNameList() {
		return fieldNameList;
	}

	public void setFieldNameList(List<String> fieldList) {
		this.fieldNameList = fieldList;
	}
	/**
	 * @return 返回字段對應的位置
	 * */
	public Integer getFieldPos(String key) {
		Integer pos = fieldPosMap.get(key);
		if(null == pos) throw new IllegalArgumentException("can't find column " + key);
		return pos;
	}
	
	public int getFiledType(int pos) {
	    return fieldTypeList[pos];
	}
	public int getFiledCount() {
		return filedCount;
	}


	public void setFiledCount(int fileCount) {
		this.filedCount = fileCount;
	}

	/**
	 * 
	 * 將字段的信息輸出到buffer中
	 * */
	public byte write(byte packetId, ProxyBuffer buffer) {
		for(int i = 0 ; i < filedCount; i ++) {
			FieldPacket fieldPacket = PacketUtil.getField(fieldNameList.get(i), fieldTypeList[i]);
			fieldPacket.packetId = packetId ++;
			fieldPacket.write(buffer);
		}
		return packetId;
	}
	
	
}
