package io.mycat.mycat2.cmds.manager;

public class ParseContext {
	
	public int sqltype;
	
	public String sql;
	
	public int offset = 6;    // start with  6. 'mycat ' has 6 chars;

	public ParseContext(String sql) {
		this.sql = sql;
	}
}
