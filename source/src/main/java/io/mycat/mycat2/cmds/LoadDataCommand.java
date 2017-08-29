package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行load data的命令处理
 * 
 * @author wuzhihui
 *
 */
public class LoadDataCommand implements SQLCommand {

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
	private byte[] overFlag = new byte[FLAGLENGTH];

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

		// 进行传输，并检查返回结果检查 ，当传输完成，就将切换为正常的透传
		if (transLoadData(session)) {
			session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
			// 当load data的包完成后，则又重新打开包完整性检查
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey());
		}

		return false;
	}

	/**
	 * 进行load data的数据传输操作
	 * 
	 * @param session
	 *            会话标识
	 * @param backresReceived
	 *            是否为后端报文标识
	 * @return true 当前传输结束
	 * @throws IOException
	 */
	public boolean transLoadData(MycatSession session) throws IOException {

		ProxyBuffer curBuffer = session.proxyBuffer;

		// 进行结束符的读取
		this.readOverByte(curBuffer);

		// 切换buffer 读写状态
		curBuffer.flip();
		// 检查当前是否结束
		if (checkOver()) {

			// 没有读取,直接透传时,需要指定 透传的数据 截止位置
			curBuffer.readIndex = curBuffer.writeIndex;

			MySQLSession mycatSession = session.getBackend();

			// 读取结束后 改变 owner，对端Session获取，并且感兴趣写事件
			session.giveupOwner(SelectionKey.OP_READ);
			mycatSession.writeToChannel();
			// 完成后，需要将buffer切换为写入事件,读取后端的数据
			curBuffer.flip();

			return true;
		} else {
			// 没有读取,直接透传时,需要指定 透传的数据 截止位置
			curBuffer.readIndex = curBuffer.writeIndex;

			// 将控制权交给后端
			session.giveupOwner(SelectionKey.OP_READ);
			MySQLSession mysqlSession = session.getBackend();
			mysqlSession.writeToChannel();

			// 然后又将后端的事件改变为前端的
			session.takeOwner(SelectionKey.OP_READ);

			// 完成后，需要将buffer切换为写入事件,读取前端的数据
			curBuffer.flip();

			return false;
		}

	}

	/**
	 * 进行结束符的读取
	 * 
	 * @param curBuffer
	 *            buffer数组信息
	 */
	private void readOverByte(ProxyBuffer curBuffer) {
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
	private boolean checkOver() {
		for (int i = 0; i < overFlag.length - 1; i++) {
			if (overFlag[i] != 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {

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
		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
