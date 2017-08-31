package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.beans.DNBean;
import io.mycat.mycat2.beans.MySQLDataSource;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.RandomUtil;

/**
 * 前端连接会话
 * 
 * @author wuzhihui
 *
 */
public class MycatSession extends AbstractMySQLSession {

	private MySQLSession backend;

	public SQLCommand curSQLCommand;
	/**
	 * Mycat Schema
	 */
	public SchemaBean schema;

	public MycatSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException {
		super(bufPool, nioSelector, frontChannel);

	}

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		// boolean usingCompress = MycatServer.getInstance().getConfig()
		// .getSystem().getUseCompression() == 1;
		// if (usingCompress) {
		// flag |= Capabilities.CLIENT_COMPRESS;
		// }
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		return flag;
	}

	/**
	 * 给客户端（front）发送认证报文
	 *
	 * @throws IOException
	 */
	public void sendAuthPackge() throws IOException {
		// 生成认证数据
		byte[] rand1 = RandomUtil.randomBytes(8);
		byte[] rand2 = RandomUtil.randomBytes(12);

		// 保存认证数据
		byte[] seed = new byte[rand1.length + rand2.length];
		System.arraycopy(rand1, 0, seed, 0, rand1.length);
		System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
		this.seed = seed;

		// 发送握手数据包
		HandshakePacket hs = new HandshakePacket();
		hs.packetId = 0;
		hs.protocolVersion = Version.PROTOCOL_VERSION;
		hs.serverVersion = Version.SERVER_VERSION;
		hs.threadId = this.getSessionId();
		hs.seed = rand1;
		hs.serverCapabilities = getServerCapabilities();
		// hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
		hs.serverStatus = 2;
		hs.restOfScrambleBuff = rand2;
		hs.write(proxyBuffer);
		// 设置frontBuffer 为读取状态
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		this.writeToChannel();
	}

	/**
	 * 当前操作的后端会话连接
	 * 
	 * @return
	 */
	public MySQLSession getBackend() {
		return backend;
	}

	/**
	 * 绑定后端MySQL会话
	 * 
	 * @param backend
	 */
	public void bindBackend(MySQLSession backend) {
		this.backend = backend;
		backend.setMycatSession(this);
		backend.useSharedBuffer(this.proxyBuffer);
		backend.setCurNIOHandler(this.getCurNIOHandler());
	}

	/**
	 * 获取ProxyBuffer控制权，同时设置感兴趣的事件，如SocketRead，Write，只能其一
	 * 
	 * @param intestOpts
	 * @return
	 */
	public void takeOwner(int intestOpts) {
		this.curBufOwner = true;
		if (intestOpts == SelectionKey.OP_READ) {
			this.change2ReadOpts();
		} else {
			this.change2WriteOpts();
		}
		if (this.backend != null) {
			backend.setCurBufOwner(false);
			backend.clearReadWriteOpts();
		}
	}

	public void chnageBothReadOpts()
	{
		this.change2ReadOpts();
		this.backend.change2ReadOpts();
	}
	/**
	 * 放弃控制权，同时设置对端MySQLSession感兴趣的事件，如SocketRead，Write，只能其一
	 * 
	 * @param intestOpts
	 */
	public void giveupOwner(int intestOpts) {
		this.curBufOwner = false;
		this.clearReadWriteOpts();
		backend.setCurBufOwner(true);
		if (intestOpts == SelectionKey.OP_READ) {
			backend.change2ReadOpts();
		} else {
			backend.change2WriteOpts();
		}
	}

	/**
	 * 向前端发送数据报文,需要先确定为Write状态并确保写入位置的正确（frontBuffer.writeState)
	 *
	 * @param rawPkg
	 * @throws IOException
	 */
	public void answerFront(byte[] rawPkg) throws IOException {
		proxyBuffer.writeBytes(rawPkg);
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		writeToChannel();
	}

	public void close(boolean normal, String hint) {
		super.close(normal, hint);
		this.curSQLCommand.clearResouces(true);
	}

	public MySQLDataSource getDatasource() {
		SchemaBean schemaBean = this.schema;
		MycatConfig mycatConf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
		if (schemaBean == null) {
			schemaBean = mycatConf.getDefaultMycatSchema();
		}
		DNBean dnBean = schemaBean.getDefaultDN();
		String replica = dnBean.getMysqlReplica();
		MySQLReplicatSet repSet = mycatConf.getMySQLReplicatSet(replica);
		MySQLDataSource datas = repSet.getCurWriteDH();
		return datas;
	}

	@Override
	protected void doTakeReadOwner() {
		this.takeOwner(SelectionKey.OP_READ);

	}

}
