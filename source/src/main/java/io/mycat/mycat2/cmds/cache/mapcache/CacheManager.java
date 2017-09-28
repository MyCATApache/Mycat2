package io.mycat.mycat2.cmds.cache.mapcache;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import io.mycat.mycat2.beans.SqlCacheBean;
import io.mycat.proxy.ProxyBuffer;

/**
 * 用于进行缓存的管理操作
 * 
 * @since 2017年9月12日 上午12:56:20
 * @version 0.0.1
 * @author liujun
 */
public class CacheManager {

	/**
	 * 缓存管理的实例对象
	 */
	public static final CacheManager INSTANCE = new CacheManager();

	/**
	 * 缓存管理mapbean信息
	 */
	private static final ConcurrentHashMap<String, SqlCacheBean> CACHEBEANMAP = new ConcurrentHashMap<>();

	/**
	 * 检查缓存是否存在
	 * 
	 * @param cacheSql
	 * @return true 缓存已经存 false 缓存 不存在
	 */
	public boolean cacheExists(String cacheSql) {
		return CACHEBEANMAP.containsKey(cacheSql);
	}

	/**
	 * 创建一个缓存对象
	 * 
	 * @param cacheSql
	 * @param timeout
	 *            过期时间以秒为单位
	 * @param buffer
	 *            读取的buffer信息
	 * @param size
	 *            内存映射的大小
	 * @return 返回映射的hash码
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void createCache(String cacheSql, int timeout, int size) throws IOException, InterruptedException {

		if (!CACHEBEANMAP.contains(cacheSql)) {
			SqlCacheBean cacheBean = new SqlCacheBean();

			cacheBean.setProcTime(System.currentTimeMillis());
			cacheBean.setTimeOutCfg(timeout * 1000);
			cacheBean.setTimeOut(System.currentTimeMillis() + timeout * 1000);
			cacheBean.setSqlReadNum(0);
			cacheBean.setSql(cacheSql);

			// 初始化内存块影射
			cacheBean.setCacheMapFile(MapFileCacheImp.INSTANCE.createCacheFile(cacheSql.getBytes(), size));

			SqlCacheBean result = CACHEBEANMAP.putIfAbsent(cacheSql, cacheBean);

			// 当发现已经存在相同的
			if (result != cacheBean && result != null) {
				MapFileCacheImp.INSTANCE.close(cacheBean.getCacheMapFile());
				cacheBean = null;
			}
		}
	}

	/**
	 * 向缓存中放入数据
	 * 
	 * @param cacheSql
	 *            缓存的SQL信息
	 * @param buffer
	 *            buffer信息
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void putCacheData(String cacheSql, ProxyBuffer buffer) throws Exception {
		if (CACHEBEANMAP.containsKey(cacheSql)) {
			SqlCacheBean cacheBean = CACHEBEANMAP.get(cacheSql);

			// 向缓存中写入数据
			MapFileCacheImp.INSTANCE.putCacheData(buffer, cacheBean.getCacheMapFile());
		}
	}

	/**
	 * 清理缓存中的数据
	 * 
	 * @param cacheSql
	 *            缓存的SQL信息
	 * @param buffer
	 *            buffer信息
	 * @throws Exception
	 */
	public void cleanCacheData(String cacheSql) throws Exception {
		if (CACHEBEANMAP.containsKey(cacheSql)) {
			SqlCacheBean cacheBean = CACHEBEANMAP.get(cacheSql);

			//重新设置新的缓存文件映射对象
			cacheBean.setCacheMapFile(MapFileCacheImp.INSTANCE.clean(cacheBean.getCacheMapFile()));
		}
	}

	/**
	 * 设置缓存可以或者失效
	 * 
	 * @param cacheSql
	 * @param flag
	 */
	public void setCacheAvailable(String cacheSql, boolean flag) {
		if (CACHEBEANMAP.containsKey(cacheSql)) {
			SqlCacheBean cacheBean = CACHEBEANMAP.get(cacheSql);

			cacheBean.getCacheMapFile().setCacheAvailable(flag);
		}
	}

	/**
	 * 获取缓存信息
	 * 
	 * @param cacheSql
	 *            缓存的SQL信息
	 * @return 缓存的状态，true 有效 false 失效
	 */
	public SqlCacheBean getCacheBean(String cacheSql) {
		if (CACHEBEANMAP.containsKey(cacheSql)) {
			return CACHEBEANMAP.get(cacheSql);
		}

		return null;
	}

	/**
	 * 获取缓存的数据
	 * 
	 * @param buffer
	 *            偏移的对象信息
	 * @param cacheSql
	 *            缓存的SQL信息
	 * @param offset
	 *            偏移量
	 * @return 当前的偏移量
	 * @throws IOException
	 *             异常信息
	 */
	public int getCacheValue(ProxyBuffer buffer, String cacheSql, int offset) throws IOException {
		if (CACHEBEANMAP.containsKey(cacheSql)) {
			SqlCacheBean cacheBean = CACHEBEANMAP.get(cacheSql);

			// 设置当前的时间
			cacheBean.setProcTime(System.currentTimeMillis());
			// 读取次数加1
			cacheBean.setSqlReadNum(cacheBean.getSqlReadNum() + 1);

			return MapFileCacheImp.INSTANCE.getByte(buffer, cacheBean.getCacheMapFile(), offset);
		}
		return -1;
	}

}
