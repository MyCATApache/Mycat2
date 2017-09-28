package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.ProxyBuffer;

/**
 * 检查是否需要走缓存的流程
 * 
 * @since 2017年9月18日 下午1:59:32
 * @version 0.0.1
 * @author liujun
 */
public class CacheFlowCheck implements ChainExecInf {
	
	/**
	 * 实例对象
	 */
	public static final CacheFlowCheck INSTANCE = new CacheFlowCheck();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		// 首先获取SQL
		MySQLSession mysqlSession = (MySQLSession) seqList.getSession();

		// 1,检查是否进行缓存添加的流程
		if (mysqlSession.getMycatSession().getSessionAttrMap()
				.containsKey(SessionKeyEnum.SESSION_KEY_CACHE_ADD_FLAG_KEY.getKey())) {
			// 继续进行下一个流程
			return seqList.nextExec();
		} else {
			overCheckTrans(mysqlSession);
		}

		return true;
	}

	/**
	 * 结束检查并透传操作
	 * 
	 * @param mysqlSession
	 * @throws IOException
	 */
	private void overCheckTrans(MySQLSession mysqlSession) throws IOException {
		ProxyBuffer buffer = mysqlSession.getProxyBuffer();
		// 进行当前的数据检查是否结束
		boolean nextReadFlag = false;
		do {
			// 进行报文的处理流程
			nextReadFlag = mysqlSession.currPkgProc.procssPkg(mysqlSession);
		} while (nextReadFlag);

		// 获取当前是否结束标识
		Boolean check = (Boolean) mysqlSession.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());

		MycatSession mycatSession = mysqlSession.getMycatSession();

		buffer.flip();
		// 检查到当前已经完成,执行添加操作
		if (null != check && check) {
			// 当知道操作完成后，前段的注册感兴趣事件为读取
			mycatSession.takeOwner(SelectionKey.OP_READ);
		}
		// 未完成执行继续读取操作
		else {
			// 直接透传报文
			mycatSession.takeOwner(SelectionKey.OP_WRITE);
		}

		mycatSession.writeToChannel();
	}

}
