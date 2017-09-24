package io.mycat.mycat2.cmds.sqlCmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行缓存的文件处理
 * 
 * @since 2017年9月4日 下午6:53:05
 * @version 0.0.1
 * @author liujun
 */
public class CacheMapFileCommand implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(CacheMapFileCommand.class);

	/**
	 * 透传的实例对象
	 */
	public static final CacheMapFileCommand INSTANCE = new CacheMapFileCommand();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		ProxyBuffer curBuffer = session.proxyBuffer;

		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();
		// 1,解析当前的内容，检查是否为缓存的标识
		session.getBackend((mysqlsession, sender, success, result) -> {
			if (success) {
				// 切换buffer 读写状态
				curBuffer.flip();

				curBuffer.readIndex = curBuffer.writeIndex;
				// 读取结束后 改变 owner，对端Session获取，并且感兴趣写事件
				session.giveupOwner(SelectionKey.OP_READ);
				// 进行传输，并检查返回结果检查 ，当传输完成，就将切换为正常的透传
				mysqlsession.writeToChannel();
			}
		});

		return false;
	}
	
	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		// 向前端写完数据，前段进入读状态
		session.proxyBuffer.flip();
		session.change2ReadOpts();
		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		Boolean flag = (Boolean) session.getMycatSession().getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_LOAD_DATA_FINISH_KEY.getKey());
		// 前段数据透传完成
		if (flag) {
			logger.debug("load data finish!!!");
			// session.getMycatSession().curSQLCommand =
			// DirectPassthrouhCmd.INSTANCE;
			// 当load data的包完成后，则又重新打开包完整性检查
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey());
			// 清除临时数组
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_LOAD_OVER_FLAG_ARRAY.getKey());
			// 读取后端的数据，然后进行透传
			session.getMycatSession().giveupOwner(SelectionKey.OP_READ);
			session.proxyBuffer.flip();
		} else {
			logger.debug("load data from front!!!");
			// 前端改为读状态
			session.getMycatSession().takeOwner(SelectionKey.OP_READ);
			session.getMycatSession().proxyBuffer.flip();
		}
		return false;
	}

}
