package io.mycat.mycat2.cmd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLProcessor;
import io.mycat.proxy.ProxyRuntime;

public abstract class AbstractSQLProcessor implements SQLProcessor {
	public final static int msyql_packetHeaderSize = 4;
	public final static int mysql_packetTypeSize = 1;

	public static final boolean validateHeader(final long offset, final long position) {
		return offset + msyql_packetHeaderSize + mysql_packetTypeSize <= position;

	}

	/**
	 * 获取报文长度
	 * 
	 * @param buffer
	 *            报文buffer
	 * @param offset
	 *            buffer解析位置偏移量
	 * @param position
	 *            buffer已读位置偏移量
	 * @return 报文长度(Header长度+内容长度)
	 * @throws IOException
	 */
	public static final int getPacketLength(ByteBuffer buffer, int offset) throws IOException {
		int length = buffer.get(offset) & 0xff;
		length |= (buffer.get(++offset) & 0xff) << 8;
		length |= (buffer.get(++offset) & 0xff) << 16;
		return length + msyql_packetHeaderSize;
	}

	@SuppressWarnings("unchecked")
	public void closeSocket(MySQLSession userSession, SocketChannel channel, boolean normal, String msg) {
		ProxyRuntime.INSTANCE.getNioProxyHandler().closeSocket(userSession, channel, normal, msg);

	}
}
