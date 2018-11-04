package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.proxy.ProxyBuffer;

import java.nio.channels.SelectionKey;

/**
 * 前段事件处理
 * 
 * @since 2017年9月18日 下午4:49:35
 * @version 0.0.1
 * @author liujun
 */
public class FrontEventProc implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final FrontEventProc INSTANCE = new FrontEventProc();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {
		// 首先获取SQL
		MySQLSession mysqlSession = (MySQLSession) seqList.getSession();

		// 1,检查当前是否需写入前端
        boolean rspFront = (boolean) mysqlSession.getMycatSession().getAttrMap()
                .get(SessionKey.CACHE_WRITE_FRONT_FLAG_KEY);

		if (rspFront) {
			ProxyBuffer curBuffer = mysqlSession.proxyBuffer;

			// 切换buffer 读写状态
			curBuffer.flip();
			// curBuffer.readIndex = curBuffer.writeIndex;
			mysqlSession.getMycatSession().takeOwner(SelectionKey.OP_WRITE);
			// 进行传输，
			mysqlSession.getMycatSession().writeToChannel();
			//不论传输多少就将切换为后端的数据读取
			//mysqlSession.getMycatSession().giveupOwner(SelectionKey.OP_READ);
		}
		// 对已经写入buffer的数据进行压缩
		else {
			ProxyBuffer proBuffer = mysqlSession.proxyBuffer;
			// 标识到当前读取到的位置，然后压缩
			proBuffer.readMark = proBuffer.readIndex;
			proBuffer.compact();
		}

		// 获取当前是否结束标识
//		Boolean check = (Boolean) mysqlSession.getAttrMap()
//				.get(SessionKey.SESSION_KEY_CONN_IDLE_FLAG.getKey());
//
//		// 检查到当前已经完成
//		if (null != check && check) {
//			// 注册前段的读取事件
//			mysqlSession.getMycatSession().change2ReadOpts();
//			// 当检查到已经完成时，前段需要获得控制权
//			mysqlSession.getMycatSession().takeOwner(SelectionKey.OP_READ);
//		} else {
//			// 未完成，则清理前段的事件
//			mysqlSession.getMycatSession().clearReadWriteOpts();
//		}

		return seqList.nextExec();
	}

}
