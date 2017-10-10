package io.mycat.mycat2.HBT;


import java.nio.channels.SelectionKey;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.FetchIntoRowStream;
import io.mycat.mycat2.tasks.JoinOutRowStream;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;

public class HBTEngine {
	private static final Logger logger = LoggerFactory.getLogger(HBTEngine.class);
	public static final String MEM = null;

	public FetchIntoRowStream fetchIntoRowStream ;
	
	public HBTEngine streamOf(MySQLSession mysqlsession, SqlMeta sqlMeta, RowMeta rowMeta) {
		FetchIntoRowStream fetchIntoRowStream = new FetchIntoRowStream(mysqlsession, sqlMeta, rowMeta);
		fetchIntoRowStream.fetchStream();
		this.fetchIntoRowStream = fetchIntoRowStream;
		return this;
	}
	

	public HBTEngine joinAndOut(MySQLSession mysqlsession, SqlMeta sqlMeta, RowMeta rowMeta, JoinMeta joinMeta,
			ResultSetMeta resultHeader, MatchCallback<List<byte[]>> matchCallBack) {
		FetchIntoRowStream joinOutRowStream = new FetchIntoRowStream(mysqlsession, sqlMeta, rowMeta);
		final  FetchIntoRowStream stream = fetchIntoRowStream;
		final AsynTaskCallBack<MySQLSession> callback = fetchIntoRowStream.getCallback();
		fetchIntoRowStream.setCallback((optSession, sender, success,result) -> {
			if(success) {
				if(callback != null) {
					callback.finished(optSession, sender, success, result);
				}
				RowMeta aRowMeta = stream.getRowMeta();
				aRowMeta.print();
				String sql = joinMeta.getSql(aRowMeta, sqlMeta);
				sqlMeta.sql = sql;
				System.out.println(sql);
				joinOutRowStream.fetchStream();
				logger.info("==============call fetch b stream========call");

				//设置合并结果。
			}else{
				MycatSession session = optSession.getMycatSession();
				session.proxyBuffer.flip();
				session.takeOwner(SelectionKey.OP_WRITE);
				session.responseOKOrError((ErrorPacket)result);
			}
		});
		setJoinOutRowCallback(stream, joinOutRowStream, joinMeta, resultHeader, matchCallBack);
		this.fetchIntoRowStream = joinOutRowStream;
		return this;
	}

	private void setJoinOutRowCallback(FetchIntoRowStream stream, FetchIntoRowStream joinOutRowStream, JoinMeta joinMeta,
			ResultSetMeta resultSetMeta, MatchCallback<List<byte[]>> matchCallBack) {
		joinOutRowStream.setCallback((optSession, sender, success,result) -> {
			if(success) {
				logger.info("==============join========call");
				RowMeta aRowMeta = stream.getRowMeta();
				RowMeta bRowMeta = joinOutRowStream.getRowMeta();
				aRowMeta.print();
				RowMeta resutlRowMeta = joinMeta.join(aRowMeta, bRowMeta, resultSetMeta, matchCallBack);
				joinOutRowStream.setRowMeta(resutlRowMeta);
//				MycatSession myCatSession = optSession.getMycatSession();
//				ProxyBuffer buffer = myCatSession.proxyBuffer;
//				buffer.reset();
//				resutlRowMeta.write(buffer);
//				buffer.flip();
//				buffer.readIndex = buffer.writeIndex; 
//				myCatSession.takeOwner(SelectionKey.OP_WRITE);
//				myCatSession.writeToChannel();  
			}else{
				MycatSession session = optSession.getMycatSession();
				session.proxyBuffer.flip();
				session.takeOwner(SelectionKey.OP_WRITE);
				session.responseOKOrError((ErrorPacket)result);
			}
		});
	}
	public void print() {
		final AsynTaskCallBack<MySQLSession> callback = fetchIntoRowStream.getCallback();
		fetchIntoRowStream.setCallback((optSession, sender, success,result) -> {
			if(success) {
				if(callback != null) {
					callback.finished(optSession, sender, success, result);
				}
				logger.info("==============call result print========call");
				RowMeta bRowMeta = fetchIntoRowStream.getRowMeta();
				MycatSession myCatSession = optSession.getMycatSession();
				ProxyBuffer buffer = myCatSession.proxyBuffer;
				buffer.reset();
				bRowMeta.write(buffer);
				buffer.flip();
				buffer.readIndex = buffer.writeIndex; 
				myCatSession.takeOwner(SelectionKey.OP_WRITE);
				myCatSession.writeToChannel();  
			}else{
				MycatSession session = optSession.getMycatSession();
				session.proxyBuffer.flip();
				session.takeOwner(SelectionKey.OP_WRITE);
				session.responseOKOrError((ErrorPacket)result);
			}
		});
	}
}
