package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行load data的命令处理
 * 
 * @author wuzhihui
 *
 */
public class LoadDataCommand implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(LoadDataCommand.class);

	/**
	 * 透传的实例对象
	 */
	public static final LoadDataCommand INSTANCE = new LoadDataCommand();

	/**
	 * loaddata传送结束标识长度
	 */
	private static final int FLAGLENGTH = 4;

	/**
	 * 结束flag标识
	 */
	//private byte[] overFlag = new byte[FLAGLENGTH];

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		ProxyBuffer curBuffer = session.proxyBuffer;

		// 进行结束符的读取
		this.readOverByte(session, curBuffer);
		//检查是否传输完成
		if (checkOver(session)) {
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_LOAD_DATA_FINISH_KEY.getKey(), true);
		} else {
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_LOAD_DATA_FINISH_KEY.getKey(), false);
		}
		
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();
		
		session.getBackend((mysqlsession, sender, success,result)->{
			if(success){
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


	/*获取结束flag标识的数组*/
	private byte[] getOverFlag(MycatSession session) {
		byte[] overFlag = (byte[])session.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_LOAD_OVER_FLAG_ARRAY.getKey());
		if(overFlag != null) {
			return overFlag;
		}
		overFlag = new byte[FLAGLENGTH];
		session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_LOAD_OVER_FLAG_ARRAY.getKey(), overFlag);
		return overFlag; 
	}
	/**
	 * 进行结束符的读取
	 * 
	 * @param curBuffer
	 *            buffer数组信息
	 */
	private void readOverByte(MycatSession session, ProxyBuffer curBuffer) {
		byte[] overFlag = getOverFlag(session);
		// 获取当前buffer的最后
		ByteBuffer buffer = curBuffer.getBuffer();

		// 如果数据的长度超过了，结束符的长度，可直接提取结束符
		if (buffer.position() >= FLAGLENGTH) {
			int opts = curBuffer.writeIndex;
			buffer.position(opts - FLAGLENGTH);
			buffer.get(overFlag, 0, FLAGLENGTH);
			buffer.position(opts);
		}
		// 如果小于结束符，说明需要进行两个byte数组的合并
		else {
			int opts = curBuffer.writeIndex;
			// 计算放入的位置
			int moveSize = FLAGLENGTH - opts;
			int index = 0;
			// 进行数组的移动,以让出空间进行放入新的数据
			for (int i = FLAGLENGTH - moveSize; i < FLAGLENGTH; i++) {
				overFlag[index] = overFlag[i];
				index++;
			}
			// 读取数据
			buffer.position(0);
			buffer.get(overFlag, moveSize, opts);
			buffer.position(opts);
		}

	}

	/**
	 * 进行结束符的检查,
	 * 
	 * 数据的结束符为0,0,0,包序，即可以验证读取到3个连续0，即为结束
	 * 
	 * @return
	 */
	private boolean checkOver(MycatSession session) {
		byte[] overFlag = getOverFlag(session);
		for (int i = 0; i < overFlag.length - 1; i++) {
			if (overFlag[i] != 0) {
				return false;
			}
		}
		return true;
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		//向前端写完数据，前段进入读状态
		session.proxyBuffer.flip();
		session.change2ReadOpts();
		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		Boolean flag =  (Boolean)session.getMycatSession().getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_LOAD_DATA_FINISH_KEY.getKey());
		//前段数据透传完成
		if(flag) {
			logger.debug("load data finish!!!");
			//session.getMycatSession().curSQLCommand = DirectPassthrouhCmd.INSTANCE;
			// 当load data的包完成后，则又重新打开包完整性检查
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey());
			//清除临时数组
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_LOAD_OVER_FLAG_ARRAY.getKey());
			//读取后端的数据，然后进行透传
			session.getMycatSession().giveupOwner(SelectionKey.OP_READ);
			session.proxyBuffer.flip();
		} else {
			logger.debug("load data from front!!!");
			//前端改为读状态
			session.getMycatSession().takeOwner(SelectionKey.OP_READ);
			session.getMycatSession().proxyBuffer.flip();
		}
		return false;
	}	
}
