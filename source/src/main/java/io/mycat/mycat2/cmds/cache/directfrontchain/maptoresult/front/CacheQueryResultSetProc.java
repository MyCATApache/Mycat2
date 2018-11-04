package io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.front;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.cache.mapcache.CacheManager;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

import java.nio.channels.SelectionKey;

/**
 * 进行缓存结果集的查询发送
 * 
 * @since 2017年9月22日 下午11:38:17
 * @version 0.0.1
 * @author liujun
 */
public class CacheQueryResultSetProc implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final CacheQueryResultSetProc INSTANCE = new CacheQueryResultSetProc();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		// 检查当前查询结果集查询是否完成
		MycatSession session = (MycatSession) seqList.getSession();

		Boolean readOver = (Boolean) session.getAttrMap()
				.get(SessionKey.CACHE_READY_OVER);

		// 首先检整个响应是否结束
		if (null != readOver && readOver) {

			// 检查当前是否存在清理缓存，并发送查询操作
			if (session.getAttrMap()
					.containsKey(SessionKey.CACHE_DELETE_QUERY_FLAG_KEY)) {

				// 当发送完成，清理重新加载缓存的标识
				session.getAttrMap().remove(SessionKey.CACHE_DELETE_QUERY_FLAG_KEY);

				// 清理结束标识
				session.getAttrMap().remove(SessionKey.CACHE_READY_OVER);

				// 将缓存中的完结标识改为false，以便后续能发送查询的SQL
				session.curBackend.setBusy();

				// 获取SQL
				String selectSql = (String) session.getAttrMap()
						.get(SessionKey.CACHE_SQL_STR);

				// 标识当前缓存放入开始
				boolean upd = CacheManager.INSTANCE.begin(selectSql);

				if (upd) {
					// 首先清理之前的缓存
					// 先将数据进行清理，再进行将缓存更新
					CacheManager.INSTANCE.cleanCacheData(selectSql);

					// 打上添加缓存的标识
					// 标识当前添加缓存操作
					session.getAttrMap().put(SessionKey.CACHE_ADD_FLAG_KEY, true);

					// 标识当前缓存无需要响应前端
					session.getAttrMap().put(SessionKey.CACHE_WRITE_FRONT_FLAG_KEY,
							false);

					ProxyBuffer proxyBuf = session.proxyBuffer;

					// 清理buffer，重新组装查询的报文
					proxyBuf.reset();

					QueryPacket queryPkg = new QueryPacket();

					queryPkg.packetId = 0;
					queryPkg.packetLength = selectSql.getBytes().length;
					queryPkg.sql = selectSql;

					// 将查询的SQL写入buffer中
					queryPkg.write(proxyBuf);

					// 切换 buffer 读写状态
					proxyBuf.flip();
					// 没有读取,直接透传时,需要指定 透传的数据 截止位置
					proxyBuf.readIndex = proxyBuf.writeIndex;
					// 改变 owner，对端Session获取，并且感兴趣写事件
					session.giveupOwner(SelectionKey.OP_WRITE);
					// 后数进行写入
					session.curBackend.writeToChannel();
				}

			}
		}

		return seqList.nextExec();
	}

}
