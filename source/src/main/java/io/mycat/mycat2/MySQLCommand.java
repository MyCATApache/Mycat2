package io.mycat.mycat2;

import java.io.IOException;

import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.util.ParseUtil;

/**
 * 负责处理SQL命令
 * 
 * @author wuzhihui
 *
 * @param <T>
 */
public interface MySQLCommand  {
	/**
	 * 收到后端应答
	 * 
	 * @param session
	 *            后端MySQLSession
	 * @return
	 * @throws IOException
	 */
	public boolean onBackendResponse(MySQLSession session) throws IOException;

	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException;

	public boolean onFrontWriteFinished(MycatSession session) throws IOException;

	public boolean onBackendWriteFinished(MySQLSession session) throws IOException;

	/**
	 * 清理资源，只清理自己产生的资源（如创建了Buffer，以及Session中放入了某些对象）
	 * 
	 * @param socketClosed
	 *            是否因为Session关闭而清理资源，此时应该彻底清理
	 */
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed);

	/**
	 * 清理资源，只清理自己产生的资源（如创建了Buffer，以及Session中放入了某些对象）
	 * 
	 * @param socketClosed
	 *            是否因为Session关闭而清理资源，此时应该彻底清理
	 */
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed);

	/**
	 * 直接应答请求报文，如果是直接应答的，则此方法调用一次就完成了，如果是靠后端响应后才应答，则至少会调用两次，
	 * 
	 * @param session
	 * @return 是否完成了应答
	 */
	public boolean procssSQL(MycatSession session) throws IOException;
	
	/**
	 * 向客户端响应 错误信息
	 * @param session
	 * @throws IOException
	 */
	public default void sendErrorMsg(MycatSession session,int errno,String errMsg) throws IOException{
		ErrorPacket errPkg = new ErrorPacket();
		errPkg.packetId =  (byte) (session.proxyBuffer.getByte(session.curMSQLPackgInf.startPos 
							+ ParseUtil.mysql_packetHeader_length) + 1);
		errPkg.errno  = errno;
		errPkg.message = errMsg;
		session.proxyBuffer.reset();
		session.responseOKOrError(errPkg);
	}
}
