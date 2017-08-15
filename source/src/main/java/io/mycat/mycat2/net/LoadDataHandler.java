package io.mycat.mycat2.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.proxy.ProxyBuffer;

/**
 * 负责处理通用的SQL命令，默认情况下透传
 * 
 * @author wuzhihui
 *
 */
public class LoadDataHandler extends DefaultMycatSessionHandler {
	private static Logger logger = LoggerFactory.getLogger(LoadDataHandler.class);

	/**
	 * 工厂方法实例对象
	 */
	public static final LoadDataHandler INSTANCE = new LoadDataHandler();

	/**
	 * loaddata传送结束标识长度
	 */
	private static final int FLAGLENGTH = 4;

	/**
	 * 结束flag标识
	 */
	private byte[] overFlag = new byte[FLAGLENGTH];

	@Override
	public void onFrontRead(final MySQLSession session) throws IOException {
		boolean readed = session.readFromChannel(session.frontBuffer, session.frontChannel);
		ProxyBuffer backendBuffer = session.frontBuffer;
		if (readed == false) {
			return;
		}
		if (session.curFrontMSQLPackgInf.endPos < backendBuffer.getReadOptState().optLimit) {
			logger.warn("front contains multi package ");
		}

		// 进行传输，并检查返回结果检查 ，当传输完成，就将切换为正常的透传
		if (transLoadData(session, false)) {
			session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
			session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
		}
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
	public boolean transLoadData(MySQLSession session, boolean backresReceived) throws IOException {

		ProxyBuffer curBuffer = session.frontBuffer;
		SocketChannel curChannel = session.backendChannel;
		if (backresReceived) {// 收到后端发来的报文

			curBuffer = session.frontBuffer;
			curChannel = session.frontChannel;
			curBuffer.changeOwner(true);
		}else
		{
			curBuffer.changeOwner(false);
		}

		// 进行结束符的读取
		this.readOverByte(curBuffer);

		session.writeToChannel(curBuffer, curChannel);

		// 检查当前是否结束
		if (checkOver()) {
			// 结束后，切换NIO的事件
			session.modifySelectKey();
			// 清空队列
			curBuffer.reset();
			return true;
		}
		// 当前的buffer被写完之后，需要做清空处理
		if (curBuffer.writeState.optPostion == curBuffer.getBuffer().position()) {
			curBuffer.reset();
		}

		return false;
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
			int opts = curBuffer.readState.optLimit;
			buffer.position(opts - FLAGLENGTH);
			buffer.get(overFlag, 0, FLAGLENGTH);
			buffer.position(opts);
		}
		// 如果小于结束符，说明需要进行两个byte数组的合并
		else {
			int opts = curBuffer.readState.optLimit;
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

}
