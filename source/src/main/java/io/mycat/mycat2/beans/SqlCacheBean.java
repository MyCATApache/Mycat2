package io.mycat.mycat2.beans;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用于进行缓存的实体信息
 * 
 * @since 2017年9月12日 上午12:59:00
 * @version 0.0.1
 * @author liujun
 */
public class SqlCacheBean {

	/**
	 * sql信息
	 */
	private String sql;

	/**
	 * 到期时间配制,计算为毫秒
	 */
	private long timeOutCfg;

	/**
	 * 过期的具体时间
	 */
	private long timeOut;

	/**
	 * 缓存读取次数
	 */
	private int sqlReadNum;

	/**
	 * 操作的时间
	 */
	private long procTime;

	/**
	 * 缓存的内存映射文件信息 ，则内存映射创建完成后返回
	 */
	private SqlCacheMapFileBean cacheMapFile;

	/**
	 * 写入锁，确保只能一个结果集进行更新操作
	 */
	private AtomicBoolean writeCacheMapLock = new AtomicBoolean(false);

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public long getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(long timeOut) {
		this.timeOut = timeOut;
	}

	public int getSqlReadNum() {
		return sqlReadNum;
	}

	public void setSqlReadNum(int sqlReadNum) {
		this.sqlReadNum = sqlReadNum;
	}

	public long getProcTime() {
		return procTime;
	}

	public void setProcTime(long procTime) {
		this.procTime = procTime;
	}

	public SqlCacheMapFileBean getCacheMapFile() {
		return cacheMapFile;
	}

	public void setCacheMapFile(SqlCacheMapFileBean cacheMapFile) {
		this.cacheMapFile = cacheMapFile;
	}

	public long getTimeOutCfg() {
		return timeOutCfg;
	}

	public void setTimeOutCfg(long timeOutCfg) {
		this.timeOutCfg = timeOutCfg;
	}

	public AtomicBoolean getWriteCacheMapLock() {
		return writeCacheMapLock;
	}

}
