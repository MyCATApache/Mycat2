package io.mycat.mycat2;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存放当前执行的SQL的相关状态属性，类似Servlet HTTP Request级别的状态，生命周期与当前SQL执行的生命周期相同。
 * 当前SQL执行完毕后清理，相关的SQLCommand ,NIOHandler等需要关注清理事件
 * 一些非常频繁使用的状态数据，可以作为CurSQLState的属性定义，加速访问
 * 
 * @author leader us
 */

public class CurSQLState {
	protected static Logger logger = LoggerFactory.getLogger(CurSQLState.class);
	/**
	 * Load Data逻辑所用
	 */
	public static final Short LOAD_OVER_FLAG_ARRAY = 1001;
	/**
	 * HBT所用：前端输出的对象table
	 */
	public static final Short HBT_TABLE_META = 2001;

	private final Map<Short, Object> requestAttrMap = new HashMap<>();

	/**
	 * 放入一个当前SQL相关的状态数据
	 * 
	 * @param key
	 * @param value
	 */
	public void set(Short key, Object value) {
		logger.debug("added sql state ,key {},value {} ", key, value);
		requestAttrMap.put(key, value);
	}

	/**
	 * 是否存在某个状态数据
	 * 
	 * @param key
	 * @return
	 */
	public boolean exisitsKey(Short key) {
		return requestAttrMap.containsKey(key);
	}

	public Object get(Short key) {
		return requestAttrMap.get(key);
	}

	public void remove(Short key) {
		logger.debug("remove sql state key {}", key);
		requestAttrMap.remove(key);

	}

	/**
	 * 清理所有状态数据
	 */
	public void clear() {
		logger.debug("clear sql state,total entries  ", requestAttrMap.size());
		requestAttrMap.clear();
	}

}
