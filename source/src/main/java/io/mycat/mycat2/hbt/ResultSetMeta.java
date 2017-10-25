package io.mycat.mycat2.hbt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mycat.mysql.packet.FieldPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.PacketUtil;

public class ResultSetMeta extends Meta {
	
	private List<String> fieldNameList;
	private Map<String,Integer> fieldPosMap;
	private int[] fieldTypeList;
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
	
	
	public void addFiled(String fieldName, int fieldType) {
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


	public byte write(byte packetId, ProxyBuffer buffer) {
		for(int i = 0 ; i < filedCount; i ++) {
			FieldPacket fieldPacket = PacketUtil.getField(fieldNameList.get(i), fieldTypeList[i]);
			fieldPacket.packetId = packetId ++;
			fieldPacket.write(buffer);
		}
		return packetId;
	}
	
	
}
