package io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.largeresult;

import java.nio.channels.SelectionKey;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.cache.mapcache.CacheManager;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKeyEnum;

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

		Boolean readOver = (Boolean) session.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CACHE_READY_OVER.getKey());

		// 首先检查读取是否完成
		if (null != readOver && readOver) {
			session.proxyBuffer.reset();

			// 获取SQL
			String selectSql = (String) session.getSessionAttrMap()
					.get(SessionKeyEnum.SESSION_KEY_CACHE_SQL_STR.getKey());
			
			//提交缓存修改操作
			CacheManager.INSTANCE.commit(selectSql);
			// session.proxyBuffer.flip();
			// 完成后，切换为读取
			// session.takeOwner(SelectionKey.OP_READ);
			session.change2ReadOpts();
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_GET_OFFSET_FLAG.getKey());
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CACHE_GET_FLAG.getKey());
			// 移除数据缓存的偏移信息
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_GET_OFFSET_FLAG.getKey());

			// 检查是否需要更新缓存缓存操作
			// 打上标识，当响应前段完成后，进行缓存的清理
			if (session.getSessionAttrMap()
					.containsKey(SessionKeyEnum.SESSION_KEY_CACHE_DELETE_QUERY_FLAG_KEY.getKey())) {
				// 添加标识当前
				// 检查标识是否需要向后端发送缓存更新的SQL语句
				return seqList.nextExec();

			}
			// 如果不需要响应则切换回透传流程处理
			else {

				// 清理结束标识
				session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CACHE_READY_OVER.getKey());
				// 当完成之后，切换回透传流程处理
				session.getCmdChain().setTarget(DirectPassthrouhCmd.INSTANCE);
				return true;
			}

		} else {

			// 未完成，则检查偏移
			long offset = 0;
			if (session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_KEY_GET_OFFSET_FLAG.getKey())) {
				offset = (long) session.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_GET_OFFSET_FLAG.getKey());
			}

			if (offset > 0) {
				// 标识当前从缓存中提取数据
				session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_GET_FLAG.getKey(), true);

				return seqList.nextExec();

			} else {

				Boolean check = (Boolean) session.curBackend.getSessionAttrMap()
						.get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());

				// 检查到当前已经完成,执行添加操作
				if (null != check && check) {
					session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_GET_OFFSET_FLAG.getKey());
					session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CACHE_GET_FLAG.getKey());
					// 移除数据缓存的偏移信息
					session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_GET_OFFSET_FLAG.getKey());
					// 清理结束标识
					session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CACHE_READY_OVER.getKey());

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
