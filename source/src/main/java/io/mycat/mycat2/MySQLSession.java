package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.buffer.BufferPool;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import static io.mycat.mycat2.MySQLSession.ResponseState.COM_QUERY;

/**
 * 后端MySQL连接
 *
 * @author wuzhihui
 *
 */
public class MySQLSession extends AbstractMySQLSession {

	private String database;
	/**
	 * 当前所从属的mycat sesssion
	 */
	private MycatSession mycatSession;

	// 记录当前后端连接所属的MetaBean，用于后端连接归还使用
	private MySQLMetaBean mysqlMetaBean;


	public MySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel, SelectionKey.OP_CONNECT);
	}

	public MycatSession getMycatSession() {
		return mycatSession;
	}

	public void bind2MycatSession(MycatSession mycatSession) {
		this.useSharedBuffer(mycatSession.getProxyBuffer());
		this.mycatSession = mycatSession;
	}

	public ResponseState responseState;
	public void startResponse() {
		this.responseState = ResponseState.COM_QUERY;
	}

	public boolean next(byte pkgType) {
		switch (this.responseState) {
			case COM_QUERY: {
				if (pkgType == MySQLPacket.ERROR_PACKET) {
					this.responseState = ResponseState.RESULT_SET_ERR;
					logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
					return true;
				}
				if (pkgType == MySQLPacket.OK_PACKET) {
					this.responseState = ResponseState.RESULT_SET_OK;
					logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
					return true;
				}
				if (pkgType == MySQLPacket.EOF_PACKET) {
					this.responseState = ResponseState.RESULT_SET_FIRST_EOF;
					logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
				}
				return false;
			}
			case RESULT_SET_FIRST_EOF: {//进入row状态
				if (pkgType == RowDataPacket.EOF_PACKET) {
					this.responseState = ResponseState.RESULT_SET_SECOND_EOF;
					logger.debug("from {} meet {} to {} ", ResponseState.RESULT_SET_FIRST_EOF, pkgType, this.responseState);
					return true;
				}
				if (pkgType == RowDataPacket.ERROR_PACKET) {
					this.responseState = ResponseState.RESULT_SET_ERR;
					logger.debug("from {} meet {} to {} ", ResponseState.RESULT_SET_FIRST_EOF, pkgType, this.responseState);
					return true;
				}
				return false;
			}
			default:
				throw new RuntimeException("unknown state!");
		}
	}

	public boolean isResponseFinished() {
		return this.responseState.isFinished();
	}

	public boolean isResponseRowData() {
		return this.responseState == ResponseState.RESULT_SET_FIRST_EOF;
	}

	/**
	 * 该方法 仅限 mycatsession 调用。 心跳时，请从mycatSession 解除绑定
	 */
	public void unbindMycatSession() {
		this.useSharedBuffer(null);
		this.setCurBufOwner(true); // 设置后端连接 获取buffer 控制权
		if (this.mycatSession != null) {
			this.mycatSession.clearBeckend(this);
		}
		this.mycatSession = null;
		this.setIdle();
	}


	public enum ResponseState {
		COM_QUERY,
		RESULT_SET_FIRST_EOF,
		RESULT_SET_SECOND_EOF,
		RESULT_SET_ERR,
		RESULT_SET_OK;

		public boolean isStart() {
			return this == COM_QUERY;
		}

		public boolean isFinished() {
			return !isStart() && this != RESULT_SET_FIRST_EOF;
		}
	}
	@Override
	public void close(boolean normal, String hint) {
		super.close(normal, hint);
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setMycatSession(MycatSession mycatSession) {
		this.mycatSession = mycatSession;
	}

	@Override
	protected void doTakeReadOwner() {
		this.getMycatSession().takeOwner(SelectionKey.OP_READ);
	}

	public MySQLMetaBean getMySQLMetaBean() {
		return mysqlMetaBean;
	}

	public void setMySQLMetaBean(MySQLMetaBean metaBean) {
		this.mysqlMetaBean = metaBean;
	}

	@Override
	public String toString() {
		return "MySQLSession [sessionId = " + getSessionId() + " , database=" + database + ", ip="
				+ mysqlMetaBean.getDsMetaBean().getIp() + ",port=" + mysqlMetaBean.getDsMetaBean().getPort()
				+ ",hashCode=" + this.hashCode() + "]";
	}
}
