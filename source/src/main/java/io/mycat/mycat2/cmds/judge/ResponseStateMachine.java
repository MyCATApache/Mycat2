package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.mycat2.cmds.judge.ResponseStateMachine.PacketState.COM_QUERY;

public class ResponseStateMachine {
    int serverStatus;
    boolean isCommandFinished = false;
    private MySQLSession sqlSession;
    protected static Logger logger = LoggerFactory.getLogger(ResponseStateMachine.class);

    public ResponseStateMachine(MySQLSession sqlSession) {
        this.sqlSession = sqlSession;
    }
    public enum PacketState {
        COM_QUERY,
        RESULT_SET_FIRST_EOF,
        RESULT_SET_SECOND_EOF,
        RESULT_ERR,
        RESULT_OK;

        public boolean isStart() {
            return this == COM_QUERY;
        }

        public boolean isFinished() {
            return !isStart() && this != RESULT_SET_FIRST_EOF;
        }
    }

    public PacketState responseState;

    public void reset() {
        this.responseState = COM_QUERY;
    }

    public boolean isInteractive(){
        return isInteractive(serverStatus);
    }
    public static boolean isInteractive(int serverStatus){
        return JudgeUtil.hasTrans(serverStatus)||JudgeUtil.hasFatch(serverStatus);
    }
    public boolean on(byte pkgType, ProxyBuffer buffer, MySQLSession sqlSession) {
        if (pkgType == MySQLPacket.EOF_PACKET) {
            buffer.readIndex = sqlSession.curMSQLPackgInf.startPos;
            EOFPacket eofPacket = new EOFPacket();
            eofPacket.read(buffer);
            serverStatus = eofPacket.status;
        } else if (pkgType == MySQLPacket.OK_PACKET) {
            buffer.readIndex = sqlSession.curMSQLPackgInf.startPos;
            OKPacket okPacket = new OKPacket();
            okPacket.read(buffer);
            serverStatus = okPacket.serverStatus;
        }
         isCommandFinished = on(pkgType, JudgeUtil.hasMoreResult(serverStatus), JudgeUtil.hasMulitQuery(serverStatus));
        logger.debug("cmd finished:{}",isCommandFinished);
        return isCommandFinished;
    }

    public boolean on(byte pkgType, boolean moreResults, boolean moreResultSets) {
        switch (this.responseState) {
            case COM_QUERY: {
                if (pkgType == MySQLPacket.OK_PACKET) {
                    this.responseState = PacketState.RESULT_OK;
                    logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
                    return true;
                } else if (pkgType == MySQLPacket.ERROR_PACKET) {
                    this.responseState = PacketState.RESULT_ERR;
                    logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
                    return true;
                }
                if (pkgType == MySQLPacket.EOF_PACKET) {
                    this.responseState = PacketState.RESULT_SET_FIRST_EOF;
                    logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
                }
                return false;
            }
            case RESULT_SET_FIRST_EOF: {//进入row状态
                if (pkgType == RowDataPacket.EOF_PACKET) {
                    onRsFinish(sqlSession);
                    if (!moreResultSets) {
                        this.responseState = PacketState.RESULT_SET_SECOND_EOF;
                    } else {
                        this.responseState = COM_QUERY;
                    }
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_SET_FIRST_EOF, pkgType, this.responseState);
                    return !(moreResults || moreResultSets);
                }
                if (pkgType == RowDataPacket.ERROR_PACKET) {
                    this.responseState = PacketState.RESULT_ERR;
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_SET_FIRST_EOF, pkgType, this.responseState);
                    return true;
                }
                onRsRow(sqlSession);
                return false;
            }
            case RESULT_SET_SECOND_EOF:
                if (pkgType == RowDataPacket.OK_PACKET) {
                    this.responseState = PacketState.RESULT_OK;
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_SET_SECOND_EOF, pkgType, this.responseState);
                    return true;
                }
                if (pkgType == RowDataPacket.ERROR_PACKET) {
                    this.responseState = PacketState.RESULT_ERR;
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_SET_SECOND_EOF, pkgType, this.responseState);
                    return true;
                }
            default:
                logger.debug("from {} meet {} to {} ", this.responseState, pkgType, this.responseState);
                throw new RuntimeException("unknown state!");
        }
    }

    public boolean isFinished() {
        return this.isCommandFinished;
    }

    public boolean isRowData() {
        return this.responseState == PacketState.RESULT_SET_FIRST_EOF;
    }

    public void onRsColCount(MySQLSession session) {

    }

    public void onRsColDef(MySQLSession session) {

    }

    public void onRsRow(MySQLSession session) {

    }

    public void onRsFinish(MySQLSession session) {

    }
}
