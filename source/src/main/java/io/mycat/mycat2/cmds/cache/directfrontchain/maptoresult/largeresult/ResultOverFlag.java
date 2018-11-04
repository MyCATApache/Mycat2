package io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.largeresult;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.cache.mapcache.CacheManager;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;

import java.nio.channels.SelectionKey;

/**
 * 检查当前结果集是否响应完毕
 * 
 * @since 2017年9月23日 上午11:25:41
 * @version 0.0.1
 * @author liujun
 */
public class ResultOverFlag implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final ResultOverFlag INSTANCE = new ResultOverFlag();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		MycatSession session = (MycatSession) seqList.getSession();

		Boolean readOver = (Boolean) session.getAttrMap()
				.get(SessionKey.CACHE_READY_OVER.getKey());

		// 首先检查读取是否完成
		if (null != readOver && readOver) {
			session.proxyBuffer.reset();

			// 获取SQL
			String selectSql = (String) session.getAttrMap()
					.get(SessionKey.CACHE_SQL_STR.getKey());
			
			//提交缓存修改操作
			CacheManager.INSTANCE.commit(selectSql);
			// session.proxyBuffer.flip();
			// 完成后，切换为读取
			// session.takeOwner(SelectionKey.OP_READ);
			session.change2ReadOpts();
			session.getAttrMap().remove(SessionKey.OFFSET_FLAG);
			session.getAttrMap().remove(SessionKey.CACHE_GET_FLAG);
			// 移除数据缓存的偏移信息
			session.getAttrMap().remove(SessionKey.OFFSET_FLAG);

			// 检查是否需要更新缓存缓存操作
			// 打上标识，当响应前段完成后，进行缓存的清理
			if (session.getAttrMap()
					.containsKey(SessionKey.CACHE_DELETE_QUERY_FLAG_KEY)) {
				// 添加标识当前
				// 检查标识是否需要向后端发送缓存更新的SQL语句
				return seqList.nextExec();

			}
			// 如果不需要响应则切换回透传流程处理
			else {

				// 清理结束标识
				session.getAttrMap().remove(SessionKey.CACHE_READY_OVER);
				// 当完成之后，切换回透传流程处理
				session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
				return true;
			}

		} else {

			// 未完成，则检查偏移
			long offset = 0;
			if (session.getAttrMap().containsKey(SessionKey.OFFSET_FLAG)) {
				offset = (long) session.getAttrMap().get(SessionKey.OFFSET_FLAG);
			}

			if (offset > 0) {
				// 标识当前从缓存中提取数据
				session.getAttrMap().put(SessionKey.CACHE_GET_FLAG, true);

				return seqList.nextExec();

			} else {

				Boolean check = session.curBackend.isIdle();

				// 检查到当前已经完成,执行添加操作
				if (null != check && check) {
					session.getAttrMap().remove(SessionKey.OFFSET_FLAG);
					session.getAttrMap().remove(SessionKey.CACHE_GET_FLAG);
					// 移除数据缓存的偏移信息
					session.getAttrMap().remove(SessionKey.OFFSET_FLAG);
					// 清理结束标识
					session.getAttrMap().remove(SessionKey.CACHE_READY_OVER);

				}

				// 获取当前是否结束标识
				session.proxyBuffer.flip();
				// 当写入完成，前段需要交出控制权，以让后端继续
				session.giveupOwner(SelectionKey.OP_READ);

			}

		}

		return false;
	}

}
