package io.mycat.mycat2.cmds;

import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;

/**
 * 默认的sqlcommand 处理器
 *
 */
public class DefaultSqlCommand extends DirectPassthrouhCmd {

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean doFullHalfPacket(MySQLPackageInf currPackage) {
		currPackage.crossBuffer = true;
		if(MySQLPacket.OK_PACKET==currPackage.pkgType){
			isAllFinished(true);  //当前命令处理完成.清理资源 
		}
		return false;
	}
	
	/**
	 *  短半包处理
	 * @param currPackage
	 * @return 是否继续处理下一个包
	 */
	@Override
	protected boolean doShortHalfPacket(MySQLPackageInf currPackage){
		currPackage.crossBuffer = true;
		return false;  //不再处理下一个包
	}
	
	/**
	 * 长半包处理
	 * @param currPackage
	 * @return 是否继续处理下一个包
	 */
	@Override
	protected boolean doLongHalfPacket(MySQLPackageInf currPackage) {
		currPackage.crossBuffer = true;
		return false;
	}

}
