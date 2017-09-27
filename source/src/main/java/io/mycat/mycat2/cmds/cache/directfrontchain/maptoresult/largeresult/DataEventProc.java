package io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.largeresult;

import java.nio.channels.SelectionKey;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKeyEnum;

/**
 * 数据结束的事件处理
 * 
 * @since 2017年9月23日 上午11:31:37
 * @version 0.0.1
 * @author liujun
 */
public class DataEventProc implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final DataEventProc INSTANCE = new DataEventProc();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		MycatSession session = (MycatSession) seqList.getSession();

		// 重新标识market以便进行传输
		session.proxyBuffer.readMark = 0;

		// 获取当前是否结束标识
		Boolean check = (Boolean) session.curBackend.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());

		// 当前完成，注册读取事件
		if (null != check && check) {
			// 获取当前是否结束标识
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_READY_OVER.getKey(), true);
		}

		// 完成之后将再次注册写入事件
		session.takeOwner(SelectionKey.OP_WRITE);

		return seqList.nextExec();
	}

}
