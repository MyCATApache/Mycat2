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

	/**
	 * 标识当前后端数据透传是否结束的标识，存在此标识，标识未结束，否则即为结束
	 */
	SESSION_KEY_TRANSFER_OVER_FLAG("session_key_transfer_over_flag"),
	/*
	 * 用于load data中判断是否传输结束的标志的临时数组 结束flag标识的临时数组
	 */
	SESSION_KEY_LOAD_OVER_FLAG_ARRAY("session_key_load_over_flag_array"),
	/*
	 * 用于load data命令是否完成 结束flag标识
	 */
	SESSION_KEY_LOAD_DATA_FINISH_KEY("session_key_load_data_finish_key"),

	/**
	 * 用于标识添加缓存操作
	 */
	SESSION_KEY_CACHE_ADD_FLAG_KEY("session_key_cache_add_key"),

	/**
	 * 用于标识当前删除缓存后发送最新的查询语句操作
	 */
	SESSION_KEY_CACHE_DELETE_QUERY_FLAG_KEY("session_key_cache_delete_query_key"),

	/**
	 * 用于标识当前是否当前的缓存需要响应前端
	 */
	SESSION_KEY_CACHE_WRITE_FRONT_FLAG_KEY("session_key_cache_write_front_flag_key"),

	/**
	 * 用来标识当前的当前读取到的包是什么类型
	 */
	SESSION_KEY_PKG_TYPE_KEY("sesssion_key_pkgtype_key"),

	/**
	 * 从缓存中获取的标识
	 */
	SESSION_KEY_CACHE_GET_FLAG("session_key_cache_get_key"),

	/**
	 * 当前需要缓存的SQL信息
	 */
	SESSION_KEY_CACHE_SQL_STR("session_key_cache_sql_str"),

	/**
	 * 获取数据时的偏移量
	 */
	SESSION_KEY_GET_OFFSET_FLAG("session_key_get_offset_key"),

	/**
	 * 缓存读取结束的标识
	 */
	SESSION_KEY_CACHE_READY_OVER("session_key_read_over_flag"),

	/**
	 * 缓存过期时间配制
	 */
	SESSION_KEY_CACHE_TIMEOUT("session_key_timeout_flag"),

	/**
	 * 缓存临近过期时间配制
	 */
	SESSION_KEY_CACHE_TIMEOUT_CRITICAL("session_key_timeout_critical_flag"),
	
	
	/**
	 * 存在mycat的session中的责任链的名称 
	 */
	SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ("session_key_cache_mycat_chain_seq"),
	
	/**
	 * 前端输出的对象table
	 */
	SESSION_KEY_HBT_TABLE_META("session_key_hbt_table_meta"),

	/**
	 * 标识当前merge数据是否结束的标识，存在此标识，标识未结束，否则即为结束
	 */
	SESSION_KEY_MERGE_OVER_FLAG("session_key_merge_over_flag"),
	;

	private String key;

	SessionKeyEnum(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

}
