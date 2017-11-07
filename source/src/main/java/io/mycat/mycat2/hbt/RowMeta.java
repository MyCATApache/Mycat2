package io.mycat.mycat2.hbt;

import java.util.ArrayList;
import java.util.List;


public class RowMeta {
	public String table;
	public String alias;
	public List<byte[]> fieldValues; 
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
		this.fieldValues =  new ArrayList<byte[]>();
	}
	
	public void init(ResultSetMeta resultSetMeta) {
		this.fieldCount = resultSetMeta.getFiledCount();
		this.fieldValues = new ArrayList<byte[]>();
		headerResultSetMeta = resultSetMeta;
		
	}



	
}
