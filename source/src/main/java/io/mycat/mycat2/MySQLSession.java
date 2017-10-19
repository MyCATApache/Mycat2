package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.cmds.pkgread.PkgFirstReader;
import io.mycat.mycat2.cmds.pkgread.PkgProcess;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.BufferPool;

/**
 * 后端MySQL连接
 * 
 * @author wuzhihui
 *
 */
public class MySQLSession extends AbstractMySQLSession{

	private String database;
	/**
	 * 当前所从属的mycat sesssion
	 */
	private MycatSession mycatSession;

	// 记录当前后端连接所属的MetaBean，用于后端连接归还使用
	private MySQLMetaBean mysqlMetaBean;

	/**
	 * 当前结束检查处理的状态,默认为首包检查读取
	 */
	public PkgProcess currPkgProc = PkgFirstReader.INSTANCE;

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

	/**
	 * 该方法 仅限 mycatsession 调用。
	 * 心跳时，请从mycatSession 解除绑定
	 */
	public void unbindMycatSession() {
		this.useSharedBuffer(null);
		this.setCurBufOwner(true); //设置后端连接 获取buffer 控制权
		if(this.mycatSession != null) {
			this.mycatSession.clearBeckend(this);
		}
		this.mycatSession = null;
		this.setCmdChain(null);
		this.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
	}
	/**
	 * 用来判断该连接是否空闲.
	 * */
	public boolean isIDLE() {
		Boolean flag = (Boolean) this.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
		return (flag == null) ? true : flag;
	}
	
	@Override
	public void close(boolean normal, String hint) {
		super.close(normal, hint);
		if(this.mycatSession!=null){
			this.mycatSession.unbindBeckend(this);
		}
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
		return "MySQLSession [sessionId = "+getSessionId()+" , database=" + database + ", ip=" + mysqlMetaBean.getDsMetaBean().getIp() + ",port=" + mysqlMetaBean.getDsMetaBean().getPort()+ ",hashCode=" + this.hashCode() + "]";
	}

}
