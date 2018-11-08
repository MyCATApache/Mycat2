package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.cache.mapcache.CacheManager;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 将resultset的数据定入文件
 * 
 * @since 2017年9月22日 下午5:22:33
 * @version 0.0.1
 * @author liujun
 */
public class ResultSetDatatoMapFile implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final ResultSetDatatoMapFile INSTANCE = new ResultSetDatatoMapFile();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		// 首先获取mysqlsesssion对象
		MySQLSession mysqlSession = (MySQLSession) seqList.getSession();

		// 如果当前为结果集结存
		int type = (int) mysqlSession.getAttrMap().get(SessionKey.PKG_TYPE_KEY);

		// 如果为查询则放入
		if (MySQLPacket.RESULTSET_PACKET == type) {

			String cacheSql = (String) mysqlSession.getMycatSession().getAttrMap()
					.get(SessionKey.CACHE_SQL_STR);

			// 此处需要进行加锁操作，以防止 多个连接同时进行缓存的更新操作
			ProxyBuffer buffer = mysqlSession.getProxyBuffer();
			// 进行缓存数据的写入
			CacheManager.INSTANCE.putCacheData(cacheSql, buffer);

			return seqList.nextExec();
		}

		return false;
	}

}
