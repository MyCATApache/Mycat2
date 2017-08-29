package io.mycat.mycat2.tasks;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;

import java.io.IOException;

/**
 * task处理结果集的模板类
 * <p>
 * Created by ynfeng on 2017/8/28.
 */
public abstract class BackendIOTaskWithResultSet<T extends AbstractMySQLSession> extends AbstractBackendIOTask<T> {
    protected ResultSetState curRSState = ResultSetState.RS_STATUS_COL_COUNT;

    @Override
    public void onSocketRead(T session) throws IOException {
        if (session.readFromChannel()) {
            for (; ; ) {
                AbstractMySQLSession.CurrPacketType currPacketType = session.resolveMySQLPackage(session.proxyBuffer, session.curMSQLPackgInf, true);
                //因为是解析所以只处理整包
                if (currPacketType == AbstractMySQLSession.CurrPacketType.Full) {
                    MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
                    switch (curRSState) {
                        case RS_STATUS_COL_COUNT:
                            onRsColCount(session);
                            curRSState = ResultSetState.RS_STATUS_COL_DEF;
                            break;
                        case RS_STATUS_COL_DEF:
                            if (curMQLPackgInf.pkgType == MySQLPacket.EOF_PACKET) {
                                curRSState = ResultSetState.RS_STATUS_ROW;
                            } else {
                                onRsColDef(session);
                            }
                            break;
                        case RS_STATUS_ROW:
                            if (curMQLPackgInf.pkgType == MySQLPacket.EOF_PACKET) {
                                curRSState = ResultSetState.RS_STATUS_FINISH;
                                onRsFinish(session);
                            } else {
                                onRsRow(session);
                            }
                            break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    abstract void onRsColCount(T session);

    abstract void onRsColDef(T session);

    abstract void onRsRow(T session);

    abstract void onRsFinish(T session);

    public enum ResultSetState {
        /**
         * 结果集第一个包
         */
        RS_STATUS_COL_COUNT,
        /**
         * 结果集列定义
         */
        RS_STATUS_COL_DEF,
        /**
         * 结果集行数据
         */
        RS_STATUS_ROW,
        /**
         * 结果集完成
         */
        RS_STATUS_FINISH;
    }
}
