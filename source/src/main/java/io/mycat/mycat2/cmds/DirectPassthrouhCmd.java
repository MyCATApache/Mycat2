package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class DirectPassthrouhCmd implements SQLCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);
	
	public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();
	
	//********** 临时处理,等待与KK 代码合并
	private static final Map<Byte,Integer> finishPackage = new HashMap<>();
	
	private Map<Byte,Integer> curfinishPackage = new HashMap<>();
	
	static{
		finishPackage.put(MySQLPacket.OK_PACKET, 1);
		finishPackage.put(MySQLPacket.ERROR_PACKET, 1);
		finishPackage.put(MySQLPacket.EOF_PACKET, 2);
	}
	//********** 临时处理,等待与KK 代码合并
	
	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException {
		if(backresReceived){
			return backendProcessor(session,session.curFrontMSQLPackgInf,session.frontChannel,session.frontBuffer);
		}else{
			return frontProcessor(session,backresReceived);
		}
	}
	
	/**
	 * 前端报文处理
	 * @param session
	 * @return
	 * @throws IOException
	 */
	private boolean frontProcessor(MySQLSession session, boolean backresReceived) throws IOException{
		curfinishPackage.putAll(finishPackage);
		ProxyBuffer curBuffer = session.frontBuffer;
		SocketChannel curChannel = session.backendChannel;
		// 切换 buffer  读写状态
		curBuffer.flip();
		// 改变 owner
		curBuffer.changeOwner(false);
		// 没有读取,直接透传时,需要指定 透传的数据 截止位置
		curBuffer.readIndex = curBuffer.writeIndex;
		//当前是前端报文处理器,如果是后端报文处理器调用,不切换Owner
		session.writeToChannel(curBuffer, curChannel);
		return false;
	}
	
	/**
	 * 后端报文处理
	 * @param session
	 * @return
	 * @throws IOException
	 */
	private boolean backendProcessor(MySQLSession session,MySQLPackageInf curMSQLPackgInf,
			SocketChannel curChannel,ProxyBuffer curBuffer)throws IOException{
		
		if(!session.readFromChannel(session.frontBuffer, session.backendChannel)){
			return false;
		}

		boolean isallfinish = false;
		boolean isContinue = true;
		while(isContinue){
			switch(session.resolveMySQLPackage(curBuffer, curMSQLPackgInf,true)){
				case Full:						
					Integer count = curfinishPackage.get(curMSQLPackgInf.pkgType);
					if(count!=null){
						if(--count==0){
							isallfinish = true;
							curfinishPackage.clear();
						}
						curfinishPackage.put(curMSQLPackgInf.pkgType, count);
					}
					if(curBuffer.readIndex == curBuffer.writeIndex){
						isContinue = false;
					}else{
						isContinue = true;
					}
					break;
				case LongHalfPacket:			
					if(curMSQLPackgInf.crossBuffer){
						//发生过透传的半包,往往包的长度超过了buffer 的长度.
						logger.debug(" readed crossBuffer LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
					}else if(!isfinishPackage(curMSQLPackgInf)){
						//不需要整包解析的长半包透传. result set  .这种半包直接透传
						curMSQLPackgInf.crossBuffer=true;
						curBuffer.readIndex = curMSQLPackgInf.endPos;
						curMSQLPackgInf.remainsBytes = curMSQLPackgInf.pkgLength-(curMSQLPackgInf.endPos - curMSQLPackgInf.startPos);
						logger.debug(" readed LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
						logger.debug(" curBuffer {}", curBuffer);
					}else{
						// 读取到了EOF/OK/ERROR 类型长半包  是需要保证是整包的.
						logger.debug(" readed finished LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
						// TODO  保证整包的机制
					}
					isContinue = false;
					break;
				case ShortHalfPacket:
					logger.debug(" readed ShortHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
					isContinue = false;
					break;
			}
		};
		
		//切换buffer 读写状态
		curBuffer.flip();
		if(isallfinish){
			curBuffer.changeOwner(true);
		}
		// 直接透传报文
		session.writeToChannel(curBuffer, curChannel);
		/**
		* 当前命令处理是否全部结束,全部结束时需要清理资源
		*/
		return false;
	}
	
	private boolean isfinishPackage(MySQLPackageInf curMSQLPackgInf)throws IOException{
		switch(curMSQLPackgInf.pkgType){
		case MySQLPacket.OK_PACKET:
		case MySQLPacket.ERROR_PACKET:
		case MySQLPacket.EOF_PACKET:
			return true;
		default:
			return false;
		}
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

}
