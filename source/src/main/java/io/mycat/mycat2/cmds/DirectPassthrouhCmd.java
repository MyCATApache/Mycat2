package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.proxy.BufferOptState;
import io.mycat.proxy.ProxyBuffer;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public abstract class DirectPassthrouhCmd implements SQLCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);
	
    private boolean isAllFinished = false;  //是否处理完成,处理完成时,需要清理资源

	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException {
		if(backresReceived){
			return backendProcessor(session);
		}else{
			return frontProcessor(session);
		}
	}
	
	/**
	 * load data 前端报文处理
	 * @param session
	 * @return
	 * @throws IOException
	 */
	private boolean frontProcessor(MySQLSession session) throws IOException{
		ProxyBuffer curBuffer = session.frontBuffer;
		SocketChannel curChannel = session.backendChannel;
		// 直接透传报文
		curBuffer.changeOwner(!curBuffer.frontUsing());
		curBuffer.flip();
		session.writeToChannel(curBuffer, curChannel);
		session.modifySelectKey();
		return false;
	}
	
	/**
	 * 后端报文处理
	 * @param session
	 * @return
	 * @throws IOException
	 */
	private boolean backendProcessor(MySQLSession session) throws IOException{
		ProxyBuffer curBuffer = session.frontBuffer;
		SocketChannel curChannel = session.frontChannel;
		boolean isContinue = true;

		do{
			switch(session.resolveMySQLPackage(curBuffer, session.curFrontMSQLPackgInf,true)){
				case Full:
					BufferOptState readState = curBuffer.readState;
					if(readState.optPostion < readState.optLimit){  //还有数据报文没有处理完,继续处理下一个数据报文
						isContinue = true;
					}else if(readState.optPostion==readState.optLimit){  // 最后一个整包
						isContinue = doFullHalfPacket(session.curFrontMSQLPackgInf);
					}
					break;
				case LongHalfPacket:
					//如果当前半包已经透传过,继续透传. 这种情况可能发生在 字段类型时 text或 blob等超大字段的情况下
					if(session.curFrontMSQLPackgInf.crossBuffer){
						isContinue = true;
						break;
					}
					//当前长半包没有发生过透传,需要调用具体的命令判断当前长半包是否可以透传
					isContinue = doLongHalfPacket(session.curFrontMSQLPackgInf);
					break;
				case ShortHalfPacket:
					//短半包不透传
					isContinue = doShortHalfPacket(session.curFrontMSQLPackgInf);
					break;
			}
		}while(isContinue);
		
		if(session.curFrontMSQLPackgInf.crossBuffer){
			// 直接透传报文
			curBuffer.flip();
			session.writeToChannel(curBuffer, curChannel);			
		}else{
			logger.warn(" current package is not passthrouth ");
		}
		
		return isAllFinished;
	}
	
	protected  void isAllFinished(boolean finished){
		this.isAllFinished = finished;
	}
	
	/**
	 *  短半包处理
	 * @param currPackage
	 * @return 是否继续处理下一个包
	 */
	protected abstract boolean doShortHalfPacket(MySQLPackageInf currPackage);
	
	/**
	 * 长半包处理
	 * @param currPackage
	 * @return 是否继续处理下一个包
	 */
	protected abstract boolean doLongHalfPacket(MySQLPackageInf currPackage);
	
	/**
	 * 整包处理
	 * @param currPackage
	 * @return 是否继续处理下一个包
	 */
	protected abstract boolean doFullHalfPacket(MySQLPackageInf currPackage);
	

}
