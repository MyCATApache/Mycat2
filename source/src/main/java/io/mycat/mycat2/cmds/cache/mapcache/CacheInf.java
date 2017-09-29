package io.mycat.mycat2.cmds.cache.mapcache;

import java.io.IOException;

import io.mycat.mycat2.beans.SqlCacheInfoBean;
import io.mycat.proxy.ProxyBuffer;

/**
 * 缓存的接口
 * 
 * @since 2017年9月4日 下午7:03:53
 * @version 0.0.1
 * @author liujun
 */
public interface CacheInf<T extends SqlCacheInfoBean> {

	/**
	 * 创建缓存的对象信息,
	 * 
	 * @param buffer
	 *            缓存的SQL字符数组
	 * @param size
	 *            缓存的大小
	 * @return 结果
	 * @throws IOException
	 */
	public T createCacheFile(byte[] buffer, int size) throws IOException, InterruptedException;

	/**
	 * 放入缓存数 据
	 * 
	 * @param buffer
	 *            缓存的数据
	 * @param cacheObject
	 *            缓存的对象信息
	 * @throws IOException
	 */
	public void putCacheData(ProxyBuffer buffer, T cacheObject) throws Exception;

	/**
	 * 获取缓存的数据
	 * 
	 * @param buffer
	 *            缓存的信息
	 * @param cacheResult
	 *            缓存的结果
	 * @param offset
	 *            偏移
	 * @return 返回读取到的偏移量
	 * @throws IOException
	 *             异常信息
	 */
	public int getByte(ProxyBuffer buffer, T cacheResult, int offset) throws IOException, InterruptedException;

}
