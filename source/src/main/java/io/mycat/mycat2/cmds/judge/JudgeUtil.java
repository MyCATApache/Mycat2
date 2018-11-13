package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.ServerStatus;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;

public class JudgeUtil {
    public static boolean judgeEOFPacket(MySQLSession session, ProxyBuffer curBuffer) {
        curBuffer.readIndex = session.curMSQLPackgInf.startPos;
        EOFPacket eofPkg = new EOFPacket();
        eofPkg.read(curBuffer);
        int serverStatus = eofPkg.status;
        if (hasResult(serverStatus)) {
            session.setIdle(false); // 标识当前处于使用中,不能结束
            return true;
        }
        if (hasFatch(serverStatus)) {
            session.setIdle(false);            // 标识当前处于使用中
            return false;                      // 读取此标识，需要等等下一次fatch请求执行
        }
        changeTrans(serverStatus, session);
        return false;
    }

    public static boolean judgeErrorPacket(MySQLSession session, ProxyBuffer curBuffer) {
        if (!session.isTrans()) { // 首先检查是否处于事务中，如果非事务中，将结识连接结束
            session.setIdle(false);
        }
        return false;
    }

    public static boolean judgeOkPacket(MySQLSession session, ProxyBuffer curBuffer) {
        curBuffer.readIndex = session.curMSQLPackgInf.startPos;
        OKPacket okpkg = new OKPacket();
        okpkg.read(curBuffer);
        int serverStatus = okpkg.serverStatus;
        if (hasResult(serverStatus)) {
            session.setIdle(false); // 标识当前处于使用中,不能结束
            return true;
        }
        changeTrans(serverStatus, session);
        return false;
    }
    public static boolean hasMulitQuery(int serverStatus) {
        return  ServerStatus.statusCheck(serverStatus, ServerStatus.MULIT_QUERY);
    }
    public static boolean hasMoreResult(int serverStatus) {
        return  ServerStatus.statusCheck(serverStatus, ServerStatus.MORE_RESULTS);
    }
    public static boolean hasResult(int serverStatus) {
        return (hasMoreResult(serverStatus) || hasMulitQuery(serverStatus));
    }

    public static boolean hasFatch(int serverStatus) {
        // 检查是否通过fatch执行的语句
        return ServerStatus.statusCheck(serverStatus, ServerStatus.CURSOR_EXISTS);
    }

    public static boolean hasTrans(int serverStatus) {
        // 检查是否通过fatch执行的语句
        boolean trans = ServerStatus.statusCheck(serverStatus, ServerStatus.IN_TRANSACTION);
        return trans;
    }

    public static void changeTrans(int serverStatus, MySQLSession session) {
        if (hasTrans(serverStatus)) {           // 如果当前事务状态被设置，连接标识为不能结束
            session.setIdle(false);            // 标识当前处于使用中
            session.setTrans(true);            // 标识当前处于事物中
        } else {
            session.setIdle(false);             // 标识当前处于闲置中,
            session.setTrans(false);          // 当发现完毕后，将标识移除
        }
    }

    public static boolean judgeCommQuerypkgType(int pkgType, MySQLSession session) {
        if (pkgType == MySQLPacket.OK_PACKET) {
            session.setPkgType(session.curMSQLPackgInf.pkgType);// 标识当前为成功或者失败的类型
            //session.removeTransferOver();
            return judgeOkPacket(session, session.proxyBuffer);
        } else if (pkgType == MySQLPacket.ERROR_PACKET) {
            session.setPkgType(session.curMSQLPackgInf.pkgType);// 标识当前为成功或者失败的类型
            //session.removeTransferOver();
            return judgeErrorPacket(session, session.proxyBuffer);
        }
        return false;
    }
}
