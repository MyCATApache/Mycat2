package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.pkgread.CommQueryHandlerResultSet;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * As of MySQL 5.7.11, COM_FIELD_LIST is deprecated and will be removed in a
	future version of MySQL. Instead, use mysql_query() to execute a SHOW
	COLUMNS statement.
 * @author yanjunli
 * 
 * COM_FIELD_LIST:
		get the column definitions of a table
		Payload
			1 			  [04] COM_FIELD_LIST
			string[NUL]   table
			string[EOF]   field wildcard
			
   COM_FIELD_LIST Response
     The response to a COM_FIELD_LIST can either be a
     
      a ERR_Packet or one or 
      
      more Column Definition packets and a closing EOF_Packet
      
 * @author yanjunli
 */
public class ComFieldListCmd extends DirectPassthrouhCmd{
	
	private static final Logger logger = LoggerFactory.getLogger(ComFieldListCmd.class);

	public static final ComFieldListCmd INSTANCE = new ComFieldListCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();
		
		session.getBackend((mysqlsession, sender, success,result)->{
			ProxyBuffer curBuffer = session.proxyBuffer;
			// 切换 buffer 读写状态
			curBuffer.flip();
			
			if(success){
				
				mysqlsession.commandHandler = CommQueryHandlerResultSet.INSTANCE;
				mysqlsession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_COLUMN_OVER.getKey(), true);
				// 没有读取,直接透传时,需要指定 透传的数据 截止位置
				curBuffer.readIndex = curBuffer.writeIndex;
				// 改变 owner，对端Session获取，并且感兴趣写事件
				session.giveupOwner(SelectionKey.OP_WRITE);
				try {
					mysqlsession.writeToChannel();
				} catch (IOException e) {
					session.closeBackendAndResponseError(mysqlsession,success,((ErrorPacket) result));
				}
			}else{
				session.closeBackendAndResponseError(mysqlsession,success,((ErrorPacket) result));
			}
		});
		return false;
	}
}
