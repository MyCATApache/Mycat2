package io.mycat.mycat2.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLDataSource;
import io.mycat.mycat2.cmds.UseCommand;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mycat2.sqlparser.NewSQLParser;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.BackendIOHandler;
import io.mycat.proxy.FrontIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.UserProxySession;

/**
 * 负责MycatSession的NIO事件，驱动SQLCommand命令执行，完成SQL的处理过程
 * 
 * @author wuzhihui
 *
 */
public class DefaultMycatSessionHandler implements FrontIOHandler<MySQLSession>, BackendIOHandler<MySQLSession> {
	public static final DefaultMycatSessionHandler INSTANCE = new DefaultMycatSessionHandler();
	private static Logger logger = LoggerFactory.getLogger(DefaultMycatSessionHandler.class);
	private NewSQLContext sqlContext = new NewSQLContext();
	private NewSQLParser sqlParser = new NewSQLParser();

	/**
	 * 进行特殊包处理的包
	 */
	private static final Map<Integer, DefaultMycatSessionHandler> PKGMAP = new HashMap<>();

	static {
		// 进行load data命令处理类
		PKGMAP.put((int) (byte) 0xfb, LoadDataHandler.INSTANCE);
	}

	@Override
	public void onFrontRead(final MySQLSession session) throws IOException {
		boolean readed = session.readFromChannel(session.frontBuffer, session.frontChannel);
		ProxyBuffer buffer = session.frontBuffer;
		if (readed == false) {
			return;
		}
		if (session.resolveMySQLPackage(buffer, session.curFrontMSQLPackgInf, false) == false) {
			// 没有读到完整报文
			return;
		}
		if (session.curFrontMSQLPackgInf.endPos < buffer.getReadOptState().optLimit) {
			logger.warn("front contains multi package ");
		}
		if (session.backendChannel == null) {
			// todo ，从连接池中获取连接，获取不到后创建新连接，
			final MySQLDataSource datas = session.getDatasource();

			logger.info("hang cur sql for  backend connection ready ");
			
			String serverIP = datas.getConfig().getIp();
			int serverPort = datas.getConfig().getPort();
			InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
			session.backendChannel = SocketChannel.open();
			session.backendChannel.configureBlocking(false);
			session.backendChannel.connect(serverAddress);
			SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT,
					session);
			session.backendKey = selectKey;
			logger.info("Connecting to server " + serverIP + ":" + serverPort);

			BackendConCreateTask authProcessor = new BackendConCreateTask(session, null);
			authProcessor.setCallback((optSession, Sender, exeSucces, retVal) -> {
				if (exeSucces) {
					// 认证成功后开始同步会话状态至后端
					syncSessionStateToBackend(session);
				} else {
					ErrorPacket errPkg = (ErrorPacket) retVal;
					optSession.responseOKOrError(errPkg, true);

				}
			});
			session.setCurNIOHandler(authProcessor);
			return;

		} else {
		  
		    

			if (session.curFrontMSQLPackgInf.pkgType == MySQLPacket.COM_QUERY) {
				byte[] sql = session.frontBuffer.getBytes(session.curFrontMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize + 1, session.curFrontMSQLPackgInf.endPos - MySQLPacket.packetHeaderSize - 1);
				sqlParser.parse(sql, sqlContext);
				if (sqlContext.hasAnnotation()) {
					//此处添加注解处理
				}
				for (int i = 0; i < sqlContext.getSQLCount(); i++) {
					switch (sqlContext.getSQLType(i)) {
						case NewSQLContext.SHOW_SQL:
							logger.info("SHOW_SQL : "+(new String(sql, StandardCharsets.UTF_8)));
							sendSqlCommand(session);
							break;
						case NewSQLContext.SET_SQL:
							logger.info("SET_SQL : "+(new String(sql, StandardCharsets.UTF_8)));
							sendSqlCommand(session);
							break;
						case NewSQLContext.SELECT_SQL:
						case NewSQLContext.INSERT_SQL:
						case NewSQLContext.UPDATE_SQL:
						case NewSQLContext.DELETE_SQL:
							logger.info("Parse SQL : "+(new String(sql, StandardCharsets.UTF_8)));
							String tbls = "";
							for (int j=0; j<sqlContext.getTableCount(); j++) {
								tbls += sqlContext.getTableName(j)+", ";
							}
							logger.info("GET Tbls : "+tbls);
						//需要单独处理的sql类型都放这里
						default:
							sendSqlCommand(session);//线直接透传
					}
				}
			} else {
				sendSqlCommand(session);
			}
		}
	}

	private void sendSqlCommand(MySQLSession session) throws IOException {
	    if(session.curFrontMSQLPackgInf.pkgType == MySQLPacket.COM_INIT_DB) {
            //    UseCommand.INSTANCE.procssSQL(session, false);
                session.curSQLCommand = UseCommand.INSTANCE;
            } 
	    // 交给SQLComand去处理
		if (session.curSQLCommand.procssSQL(session, false)) {
			session.curSQLCommand.clearResouces(false);
		}
	}

	private void syncSessionStateToBackend(MySQLSession mySQLSession) throws IOException {
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mySQLSession);
		backendSynchronzationTask.setCallback((session, sender, exeSucces, rv) -> {
			if (exeSucces) {
				// 交给SQLComand去处理
				if (session.curSQLCommand.procssSQL(session, false)) {
					session.curSQLCommand.clearResouces(false);
				}
			} else {
				ErrorPacket errPkg = (ErrorPacket) rv;
				session.responseOKOrError(errPkg, true);
			}
		});
		mySQLSession.setCurNIOHandler(backendSynchronzationTask);
	}

	public void onBackendRead(MySQLSession session) throws IOException {
		boolean readed = session.readFromChannel(session.frontBuffer, session.backendChannel);
		if (readed == false) {
			return;
		}

		ProxyBuffer backendBuffer = session.frontBuffer;

		if (session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf, false) == false) {
			// 没有读到完整报文
			return;
		}

		// 交给SQLComand去处理
		if (session.curSQLCommand.procssSQL(session, true)) {
			session.curSQLCommand.clearResouces(false);
		}

		// 检查当前的包是否需要进行特殊的处理
		DefaultMycatSessionHandler handler = PKGMAP.get(session.curFrontMSQLPackgInf.pkgType);

		if (null != handler) {
			// 设置lodata的透传执行
			session.setCurNIOHandler(handler);
		}

	}

	@Override
	public void onBackendConnect(MySQLSession userSession, boolean success, String msg) throws IOException {
		logger.warn("not handled (expected ) onBackendConnect event " + userSession.sessionInfo());
	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onFrontSocketClosed(MySQLSession userSession, boolean normal) {
		userSession.lazyCloseSession(normal,"front closed");

	}

	/**
	 * 后端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onBackendSocketClosed(MySQLSession userSession, boolean normal) {
		userSession.lazyCloseSession(normal,"backend closed ");
	}

	/**
	 * Socket IO读写过程中出现异常后的操作，通常是要关闭Session的
	 * 
	 * @param userSession
	 * @param exception
	 */
	protected void onSocketException(UserProxySession userSession, Exception exception) {
		if (exception instanceof IOException) {
			logger.warn(
					"DefaultSQLHandler handle IO error " + userSession.sessionInfo() + " " + exception.getMessage());

		} else {
			logger.warn("DefaultSQLHandler handle IO error " + userSession.sessionInfo(), exception);
		}
		userSession.close(false,"exception:" + exception.getMessage());
	}

	@Override
	public void onFrontWrite(MySQLSession session) throws IOException {
		session.writeToChannel(session.frontBuffer, session.frontChannel);

	}

	@Override
	public void onBackendWrite(MySQLSession session) throws IOException {
		session.writeToChannel(session.frontBuffer, session.backendChannel);

	}

}
