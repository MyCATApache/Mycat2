package io.mycat.mycat2.cmds.pkgread;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.AbstractMySQLSession.CurrPacketType;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.cmds.LoadDataCommand;
import io.mycat.mycat2.cmds.judge.DirectTransJudge;
import io.mycat.mycat2.cmds.judge.ErrorJudge;
import io.mycat.mycat2.cmds.judge.OkJudge;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 
 * 进行首包的读取
 * 
 * @since 2017年8月23日 下午11:09:49
 * @version 0.0.1
 * @author liujun
 */
public class PkgFirstReader implements PkgProcess {

	/**
	 * 首包处理的实例对象
	 */
	public static final PkgFirstReader INSTANCE = new PkgFirstReader();

	/**
	 * 查询包标识的开始
	 */
	private static final int QUERY_PKG_START = 0x01;

	/**
	 * 指定需要处理的包类型信息
	 */
	private static final Map<Integer, DirectTransJudge> JUDGEMAP = new HashMap<>();

	static {
		// 用来进行ok包的处理理
		JUDGEMAP.put((int) MySQLPacket.OK_PACKET, OkJudge.INSTANCE);
		// 用来进行error包的处理
		JUDGEMAP.put((int) MySQLPacket.ERROR_PACKET, ErrorJudge.INSTANCE);
	}

	@Override
	public boolean procssPkg(MySQLSession session) throws IOException {

		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;

		ProxyBuffer curBuffer = session.proxyBuffer;

		// 进行首次的报文解析
		CurrPacketType pkgTypeEnum = session.resolveMySQLPackage(curBuffer, curMSQLPackgInf, true);

		// 首包，必须为全包进行解析，否则再读取一次，进行操作
		if (null != pkgTypeEnum && CurrPacketType.Full == pkgTypeEnum) {

			int pkgType = curMSQLPackgInf.pkgType;

			// 如果当前为查询包，则切换到查询的逻辑命令处理
			if (QUERY_PKG_START <= pkgType) {

				// 当前确认查询包，则切换至查询的读取操作
				session.currPkgProc = PkgResultSetReader.INSTANCE;
				return true;
			}
			// 如果当前为特殊的load data包，则直接进行切换至load data的逻辑处理
			else if (session.curMSQLPackgInf.pkgType == MySQLPacket.LOAD_DATA_PACKET) {
				session.getMycatSession().curSQLCommand = LoadDataCommand.INSTANCE;
				// 将前端的包检查关闭
				session.getMycatSession().getSessionAttrMap().put(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey(), true);

				// 切换buffer 读状态
				curBuffer.flip();
				MycatSession mycatSession = session.getMycatSession();
				// 直接透传报文
				mycatSession.takeOwner(SelectionKey.OP_READ);
				mycatSession.writeToChannel();

				// 完成后，需要将buffer切换为写入事件,读取前端的数据
				curBuffer.flip();
			}
			// 如果为ok和error则切换到error的包判断
			else {
				DirectTransJudge judge = JUDGEMAP.get(session.curMSQLPackgInf.pkgType);
				// 当检查到为需要检查的包，则进行检查
				if (null != judge) {
					//当检查到完毕后，直接结束
					session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey());
					
					judge.judge(session);
				}

				// 切换buffer 读写状态
				curBuffer.flip();
				MycatSession mycatSession = session.getMycatSession();
				// 直接透传报文
				mycatSession.takeOwner(SelectionKey.OP_WRITE);
				mycatSession.writeToChannel();
			}
		} 
		//对于首包非完整的，透传已经检查完毕
		else {
			// 切换buffer 读写状态
			curBuffer.flip();
			MycatSession mycatSession = session.getMycatSession();
			// 直接透传报文
			mycatSession.takeOwner(SelectionKey.OP_WRITE);
			mycatSession.writeToChannel();

			// 标识当前传输未结束
			mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey(), true);
		}

		/**
		 * 当前命令处理是否全部结束,全部结束时需要清理资源
		 */
		return false;

	}

}
