package io.mycat.mycat2.net;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.Interceptor.InterceptorSystem;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.sqlannotations.AnnotationProcessor;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManager;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManagerImpl;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.SQLType;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 负责MycatSession的NIO事件，驱动SQLCommand命令执行，完成SQL的处理过程
 * 
 * @author wuzhihui
 *
 */
public class DefaultMycatSessionHandler implements NIOHandler<AbstractMySQLSession> {
	public static final DefaultMycatSessionHandler INSTANCE = new DefaultMycatSessionHandler();
	private static Logger logger = LoggerFactory.getLogger(DefaultMycatSessionHandler.class);

	public void onSocketRead(final AbstractMySQLSession session) throws IOException {
		if (session instanceof MycatSession) {
			onFrontRead((MycatSession) session);
		} else {
			onBackendRead((MySQLSession) session);
		}
	}

	private void onFrontRead(final MycatSession session) throws IOException {
		boolean readed = session.readFromChannel();
		ProxyBuffer buffer = session.getProxyBuffer();
		// 在load data的情况下，SESSION_PKG_READ_FLAG会被打开，以不让进行包的完整性检查
		if (!session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey())
				&& (readed == false ||
						// 没有读到完整报文
						MySQLSession.CurrPacketType.Full != session.resolveMySQLPackage(buffer, session.curMSQLPackgInf,
								false))) {
			return;
		}
		if (session.curMSQLPackgInf.endPos < buffer.writeIndex) {
			logger.warn("front contains multi package ");
		}
		if(MySQLPacket.COM_QUERY==(byte)session.curMSQLPackgInf.pkgType) {
			BufferSQLParser parser = new BufferSQLParser();
			int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize +1 ;
			int length = session.curMSQLPackgInf.pkgLength -  MySQLPacket.packetHeaderSize - 1 ;
			parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
			BufferSQLContext context = session.sqlContext;
			AnnotationProcessor.getInstance().parse(context,session);

		}
		session.curSQLCommand = null;
		session.clearSQLCmdMap();
		
		InterceptorSystem.INSTANCE.onFrontReadIntercept(session);

	}

	private void onBackendRead(MySQLSession session) throws IOException {
		InterceptorSystem.INSTANCE.onBackendReadIntercept(session);
	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onSocketClosed(AbstractMySQLSession session, boolean normal) {
		if (session instanceof MycatSession) {
			logger.info("front socket closed " + session);
			session.lazyCloseSession(normal, "front closed");
		} else {
			MySQLSession mysqlSession = (MySQLSession) session;
			try {
				InterceptorSystem.INSTANCE.onBackendClosedIntercept(mysqlSession, normal);
			
			} catch (IOException e) {
				logger.warn("caught err ", e);
			}
		}
	}

	@Override
	public void onSocketWrite(AbstractMySQLSession session) throws IOException {
		session.writeToChannel();

	}

	@Override
	public void onConnect(SelectionKey curKey, AbstractMySQLSession session, boolean success, String msg)
			throws IOException {
		throw new java.lang.RuntimeException("not implemented ");
	}

	@Override
	public void onWriteFinished(AbstractMySQLSession session) throws IOException {
		// 交给SQLComand去处理
		if (session instanceof MycatSession) {
			InterceptorSystem.INSTANCE.clearFrontWriteFinishedIntercept((MycatSession) session);
		} else {
			InterceptorSystem.INSTANCE.clearBackendResoucesIntercept((MySQLSession) session);
		}
	}

}
