package io.mycat.mycat2.HBT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mycat.mysql.packet.EOFPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ParseUtil;


public class RowMeta {
	public String table;
	public String alias;
	public List<List<byte[]>> fieldValues; 
	public int fieldCount;
	public ResultSetMeta headerResultSetMeta;
	public RowMeta(String table, String alias) {
		this.table = table;
		this.alias = alias;
	}
	public RowMeta() {
	}
	/**/
	public void init(int fieldCount) {
		this.fieldCount = fieldCount;
		headerResultSetMeta = new ResultSetMeta(fieldCount);
		this.fieldValues = new ArrayList<List<byte[]>>();
	}
	
	public void init(ResultSetMeta resultSetMeta) {
		this.fieldCount = resultSetMeta.getFiledCount();
		this.fieldValues = new ArrayList<List<byte[]>>();
		headerResultSetMeta = resultSetMeta;
		
	}
	
	public void addFieldValues(List<byte[]> row) {
		fieldValues.add(row);
	}

	public ResultSetMeta getHeaderResultSet() {
		return headerResultSetMeta;
	}

	public void write(ProxyBuffer buffer) {
		byte packetId = 1;
		packetId = writeResultSetHeaderPacket(packetId, buffer);
		packetId = headerResultSetMeta.write(packetId, buffer);
		packetId = writeEofPacket(packetId, buffer);
		packetId = writeRowData(packetId, buffer);
		packetId = writeEofPacket(packetId, buffer);
	}
//	
	private byte writeEofPacket(byte packetId, ProxyBuffer buffer) {
		EOFPacket eofPacket = new EOFPacket();
		eofPacket.packetId = packetId ++;
		eofPacket.warningCount = 0;
		eofPacket.write(buffer);
		return packetId;
	}

	private byte writeResultSetHeaderPacket(byte packetId, ProxyBuffer buffer) {
		buffer.writeFixInt(3, ProxyBuffer.getLenencLength(fieldCount));
		buffer.writeByte(packetId ++);
		buffer.writeLenencInt(fieldCount);
		return packetId;
	}
	
//	
	private byte writeRowData(byte packetId, ProxyBuffer buffer) {
		for(List<byte[]> fieldValue : fieldValues) {
//			buffer.writeFixInt(3, field.length);
			int tmpWriteIndex = buffer.writeIndex;
			buffer.writeIndex +=3;
			buffer.writeByte(packetId ++);
			for(byte[] value : fieldValue) {
				buffer.writeLenencBytes(value);
			}
			//写入长度
			buffer.putFixInt(tmpWriteIndex, 3, buffer.writeIndex - tmpWriteIndex - ParseUtil.msyql_packetHeaderSize);
		}
		return packetId;
	}
	public void print() {
		for(List<byte[]> fieldValue : fieldValues) {
			
			for(byte[] value : fieldValue) {
				System.out.print(String.format("%s  ", new String(value)));
			}
			System.out.println("");
		}
	}
	public String getFileds(String lJoinKey) {
		
		Integer pos = getHeaderResultSet().getFieldPos(lJoinKey);
		if(pos == null) throw new IllegalArgumentException("fileds not found joinkey " + lJoinKey);
		StringBuilder sb = new StringBuilder("");
		fieldValues.forEach(row -> {
			String value = new String(row.get(pos));
			sb.append("'").append(value).append("',");
		});
		String ids = "''";
		if(sb.length() > 1) {
			ids = sb.substring(0, sb.length() - 1);
		} 
		return ids;
		
	}

	public Map<String, List<List<byte[]>>> getKeyMap(String key) {
		Map<String, List<List<byte[]>>> fieldMap = new HashMap<String, List<List<byte[]>>>();
		fieldValues.forEach(row -> {
			int pos = headerResultSetMeta.getFieldPos(key);
			String value = new String(row.get(pos));
			List<List<byte[]>> rowDataList = fieldMap.get(value);
			if (rowDataList == null) {
				rowDataList = new ArrayList<List<byte[]>>();
				fieldMap.put(value, rowDataList);
			}
			rowDataList.add(row);
		});
		return fieldMap;
	}
	
	public List<List<byte[]>> getFieldValues() {
		return fieldValues;
	}
	public void setFieldValues(List<List<byte[]>> fieldValues) {
		this.fieldValues = fieldValues;
	}
	
}
