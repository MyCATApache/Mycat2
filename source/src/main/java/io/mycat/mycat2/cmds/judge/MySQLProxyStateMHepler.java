package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.StringUtil;

/**
 * cjw
 * 294712221@qq.com
 */
public class MySQLProxyStateMHepler {
    public static boolean on(MySQLProxyStateM sm,int pkgType, ProxyBuffer buffer, MySQLSession sqlSession) {
        int backupReadIndex = buffer.readIndex;
        boolean preparedOkPacket = false;
        if (pkgType == MySQLPacket.EOF_PACKET) {
            buffer.readIndex = sqlSession.curMSQLPackgInf.startPos;
            EOFPacket eofPacket = new EOFPacket();
            eofPacket.read(buffer);
            int old = sm.serverStatus;
            sm.serverStatus = eofPacket.status;
            sm.callback.onServerStatusChanged(sm, old, sm.serverStatus);
        } else if (pkgType == MySQLPacket.OK_PACKET) {
            buffer.readIndex = sqlSession.curMSQLPackgInf.startPos;
            OKPacket okPacket = new OKPacket();
            okPacket.read(buffer);
            preparedOkPacket = judgePreparedOkPacket(sm,buffer, sqlSession.curMSQLPackgInf);
            int old = sm.serverStatus;
            sm.serverStatus = okPacket.serverStatus;
            sm.callback.onServerStatusChanged(sm, old, sm.serverStatus);

        }
        buffer.readIndex = backupReadIndex;
        return sm.on(pkgType,sm.serverStatus,preparedOkPacket);
    }
    public static boolean judgePreparedOkPacket(MySQLProxyStateM sm,ProxyBuffer buffer, MySQLPackageInf curMSQLPackgInf) {
        //0x16 COM_STMT_PREPARE
        //@todo check or condition
        String s = StringUtil.dumpAsHex(buffer.getBuffer(), curMSQLPackgInf.startPos, curMSQLPackgInf.pkgLength);
        System.out.println(s);
        int backupReadIndex = buffer.readIndex;
        buffer.readIndex = curMSQLPackgInf.startPos;
        try {
            if (sm.commandType == 22 && buffer.readByte() == 0x0c) {
                buffer.readIndex = curMSQLPackgInf.startPos + 9;

                long prepareFieldNum = buffer.readFixInt(2);
                long prepareParamNum = buffer.readFixInt(2);
                byte b1 = buffer.readByte();
                boolean b = b1 == 0;
                if (b) {
                    sm.prepareFieldNum = prepareFieldNum == 0 ? -1 : prepareFieldNum;
                    sm.prepareParamNum = prepareParamNum == 0 ? -1 : prepareParamNum;
                }
                return b;
            }
            return false;
        } finally {
            buffer.readIndex = backupReadIndex;
            sm.commandType = 0;
        }
    }

}
