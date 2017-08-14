package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行load data的命令数据透传
 * @since 2017年8月14日 下午2:55:06
 * @version 0.0.1
 * @author liujun
 */
public class LoadDataCmd implements SQLCommand {

	/**
	 * loaddata传送结束标识长度
	 */
	private static final int FLAGLENGTH = 4;

	/**
	 * 结束flag标识
	 */
	private byte[] overFlag = new byte[FLAGLENGTH];

	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException {

		ProxyBuffer curBuffer = session.backendBuffer;
		SocketChannel curChannel = session.backendChannel;
		if (backresReceived) {// 收到后端发来的报文

			curBuffer = session.frontBuffer;
			curChannel = session.frontChannel;
		}

		// 进行结束符的读取
		this.readOverByte(curBuffer);

		session.writeToChannel(curBuffer, curChannel);

		// 检查结束切换状态
		if (checkOver()) {
			session.modifySelectKey();
			curBuffer.reset();
			return true;
		}

		curBuffer.reset();

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

	/**
	 * 测试方法，验证组合读取
	 * 
	 * @param buffer
	 * @param overFlag
	 * @return
	 */
	public static byte[] TestReadByte(ByteBuffer buffer, byte[] overFlag) {
		// byte[] overFlag = new byte[FLAGLENGTH];

		if (buffer.position() >= FLAGLENGTH) {
			int opts = buffer.position();
			buffer.position(opts - FLAGLENGTH);
			buffer.get(overFlag, 0, FLAGLENGTH);
			buffer.position(opts);
		} else {

			int opts = buffer.position();
			// 计算需要移动的位数
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

		return overFlag;
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {

	}

	public static void main(String[] args) {

		ByteBuffer buffer = ByteBuffer.allocateDirect(30);
		byte[] overFlag = new byte[FLAGLENGTH];

		buffer.put((byte) 1);
		buffer.put((byte) 2);
		buffer.put((byte) 3);
		buffer.put((byte) 4);

		overFlag = TestReadByte(buffer, overFlag);

		ByteBuffer buffer2 = ByteBuffer.allocateDirect(1);
		buffer2.put((byte) 5);

		overFlag = TestReadByte(buffer2, overFlag);

		System.out.println(Arrays.toString(overFlag));

	}

}
