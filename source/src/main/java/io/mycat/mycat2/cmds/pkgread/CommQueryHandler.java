package io.mycat.mycat2.cmds.pkgread;

import io.mycat.mycat2.AbstractMySQLSession.CurrPacketType;
import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.cmds.ComStatisticsCmd;
import io.mycat.mycat2.cmds.LoadDataCommand;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

/**
 * 用来进行comm_query(3)的类型处理
 *
 * @author liujun
 * @version 0.0.1
 * @since 2017年8月23日 下午11:09:49
 */
public class CommQueryHandler implements CommandHandler {

    /**
     * 首包处理的实例对象
     */
    public static final CommQueryHandler INSTANCE = new CommQueryHandler();

    /**
     * 查询包标识的开始
     */
    private static final int QUERY_PKG_START = 0x01;

    /**
     * 特殊命令报文,不需要判断首包，直接返回。 例如
     */
    private static final List<MySQLCommand> extendCmdPkg = new ArrayList<>();

    static {

        extendCmdPkg.add(ComStatisticsCmd.INSTANCE);
    }

    @Override
    public boolean procss(MySQLSession session) throws IOException {

        MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;

        ProxyBuffer curBuffer = session.proxyBuffer;

        // 进行首次的报文解析
        CurrPacketType pkgTypeEnum = session.resolveMySQLPackage(curBuffer, curMSQLPackgInf, true);

        // 首包，必须为全包进行解析，否则再读取一次，进行操作
        if (null != pkgTypeEnum && CurrPacketType.Full == pkgTypeEnum) {

            int pkgType = curMSQLPackgInf.pkgType;

            if (extendCmdPkg.contains(session.getMycatSession().curSQLCommand)) {
                return false;
            }

            // 如果当前为查询包，则切换到查询的逻辑命令处理
            if (QUERY_PKG_START <= pkgType) {
                session.setPkgType(MySQLPacket.RESULTSET_PACKET); // 标识当前为查询
                // 当前确认查询包，则切换至查询的读取操作
                session.getMycatSession().commandHandler = CommQueryHandlerResultSet.INSTANCE;
                return true;
            }
            // 如果当前为特殊的load data包，则直接进行切换至load data的逻辑处理
            else if (session.curMSQLPackgInf.pkgType == MySQLPacket.LOAD_DATA_PACKET) {
                // 标识当前为loaddata操作
                session.setPkgType(MySQLPacket.LOAD_DATA_PACKET);
                session.getMycatSession().curSQLCommand = LoadDataCommand.INSTANCE;
                // 将前端的包检查关闭
                session.setPkgReadFlag();
                // 切换buffer 读状态
                curBuffer.flip();
                MycatSession mycatSession = session.getMycatSession();
                // 直接透传报文
                mycatSession.takeOwner(SelectionKey.OP_READ);
                mycatSession.writeToChannel();
            }
            // 如果为ok和error则切换到error的包判断
            else {
                boolean runFlag = JudgeUtil.judgeCommQuerypkgType(session.curMSQLPackgInf.pkgType, session);
                return runFlag;
            }
        }
        // 对于首包非完整的，透传已经检查完毕
        else {
            MycatSession mycatSession = session.getMycatSession();
            // 标识当前传输未结束
            mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey(), true);
        }

        /**
         * 当前命令处理是否全部结束,全部结束时需要清理资源
         */
        return false;

    }

}
