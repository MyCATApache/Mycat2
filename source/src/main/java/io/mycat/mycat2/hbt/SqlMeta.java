package io.mycat.mycat2.hbt;

public class SqlMeta {
	/*存儲的sql 以及對應對的表的別名*/
	public String sql ;
	public String aliasTable;
	public SqlMeta(String sql, String aliasTable) {
		this.aliasTable = aliasTable;
		this.sql = sql;
	}

}
