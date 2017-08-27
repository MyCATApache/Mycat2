package io.mycat.mycat2.console;

/**
 * sesssion会话中的参数值信息
 * 
 * @since 2017年8月24日 下午11:39:43
 * @version 0.0.1
 * @author liujun
 */
public enum SessionKeyEnum {

	/**
	 * session会话中的是否需要读取验证的标识
	 */
	SESSION_PKG_READ_FLAG("session_pkg_read_flag"),

	/**
	 * 用来标识session会话中列结束标识
	 */
	SESSION_KEY_COLUMN_OVER("session_key_colum_over"),

	/**
	 * 用来标识session会话中事务
	 */
	SESSION_KEY_TRANSACTION_FLAG("session_key_transaction_flag"),
	
	
	/**
	 * 标识当前连接的闲置状态标识 ，true，闲置，false，未闲置,即在使用中
	 */
	SESSION_KEY_CONN_IDLE_FLAG("session_key_conn_idle_flag"),

	;

	private String key;

	private SessionKeyEnum(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

}
