package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * Get a human readable string of internal statistics.
	COM_STATISTICS:
	get a list of active threads
	Returns
	string.EOF
 * @author yanjunli
 *
 */
public class ComStatisticsCmd extends DirectPassthrouhCmd{
	
	private static final Logger logger = LoggerFactory.getLogger(ComStatisticsCmd.class);

	public static final ComStatisticsCmd INSTANCE = new ComStatisticsCmd();
	
	private ComStatisticsCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

		logger.error("com_statistics  command is not implement!!!! please  fix it");
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();
		
		session.getBackend((mysqlsession, sender, success,result)->{
			ProxyBuffer curBuffer = session.proxyBuffer;
			// 切换 buffer 读写状态
			curBuffer.flip();
			
			if(success){
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
