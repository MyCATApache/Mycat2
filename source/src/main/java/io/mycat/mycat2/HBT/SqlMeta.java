package io.mycat.mycat2.HBT;

public class SqlMeta {

	public String sql ;
	public String aliasTable;
	public SqlMeta(String sql, String aliasTable) {
		this.aliasTable = aliasTable;
		this.sql = sql;
	}

}
