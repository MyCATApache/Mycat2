package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.mycat2.cmds.judge.ResponseStateMachine.PacketState.COM_QUERY;

/**
 * cjw
 */
public class ResponseStateMachine {
    int serverStatus;
    boolean isCommandFinished = false;
    byte commandType;//prepared
    long prepareFieldNum;
    long prepareParamNum;
    private MySQLSession sqlSession;

    public byte getCommandType() {
        return commandType;
    }


    protected static Logger logger = LoggerFactory.getLogger(ResponseStateMachine.class);

    public ResponseStateMachine(MySQLSession sqlSession) {
        this.sqlSession = sqlSession;
    }

    public enum PacketState {
        COM_QUERY,
        RESULT_SET_FIRST_EOF,
        RESULT_SET_SECOND_EOF,
        RESULT_ERR,
        RESULT_OK,
        CLOSE_STATMENT,
        PREPARED;

        public boolean isStart() {
            return this == COM_QUERY;
        }

        public boolean isFinished() {
            return !isStart() && this != RESULT_SET_FIRST_EOF;
        }
    }

    public PacketState responseState;

    public void reset(byte commandType) {
        this.commandType = commandType;
        this.responseState = COM_QUERY;
        this.isCommandFinished = false;
        this.prepareFieldNum = 0;
        this.prepareParamNum = 0;
        switch (commandType) {
            case 25:
                this.isCommandFinished = true;//Request Command Close Statement
            default:

        }
    }

    public boolean isInteractive() {
        return isInteractive(serverStatus);
    }

    public static boolean isInteractive(int serverStatus) {
        return JudgeUtil.hasTrans(serverStatus) || JudgeUtil.hasFatch(serverStatus);
    }

    public boolean judgePreparedOkPacket(ProxyBuffer buffer, MySQLPackageInf curMSQLPackgInf) {
        //0x16 COM_STMT_PREPARE
        //@todo check or condition
        String s = StringUtil.dumpAsHex(buffer.getBuffer(), curMSQLPackgInf.startPos, curMSQLPackgInf.pkgLength);
        System.out.println(s);
        int backupReadIndex = buffer.readIndex;
        buffer.readIndex = curMSQLPackgInf.startPos;
        try {
            if (commandType == 22 && buffer.readByte() == 0x0c) {
                buffer.readIndex = curMSQLPackgInf.startPos + 9;

                long prepareFieldNum = buffer.readFixInt(2);
                long prepareParamNum = buffer.readFixInt(2);
                byte b1 = buffer.readByte();
                boolean b = b1 == 0;
                if (b) {
                    this.prepareFieldNum = prepareFieldNum == 0 ? -1 : prepareFieldNum;
                    this.prepareParamNum = prepareParamNum == 0 ? -1 : prepareParamNum;
                }
                return b;
            }
            return false;
        } finally {
            buffer.readIndex = backupReadIndex;
            commandType = 0;
        }
    }

    public boolean on(byte pkgType, ProxyBuffer buffer, MySQLSession sqlSession) {
        int backupReadIndex = buffer.readIndex;
        boolean preparedOkPacket = false;
        if (pkgType == MySQLPacket.EOF_PACKET) {
            buffer.readIndex = sqlSession.curMSQLPackgInf.startPos;
            EOFPacket eofPacket = new EOFPacket();
            eofPacket.read(buffer);
            serverStatus = eofPacket.status;
        } else if (pkgType == MySQLPacket.OK_PACKET) {
            buffer.readIndex = sqlSession.curMSQLPackgInf.startPos;
            OKPacket okPacket = new OKPacket();
            okPacket.read(buffer);
            preparedOkPacket = judgePreparedOkPacket(buffer, sqlSession.curMSQLPackgInf);
            serverStatus = okPacket.serverStatus;
        }
        buffer.readIndex = backupReadIndex;
        isCommandFinished = on(pkgType, JudgeUtil.hasMoreResult(serverStatus), JudgeUtil.hasMulitQuery(serverStatus), preparedOkPacket);
        logger.debug("cmd finished:{}", isCommandFinished);
        return isCommandFinished;
    }

    public boolean on(int pkgType, boolean moreResults, boolean moreResultSets, boolean preparedOkPacket) {
        switch (this.responseState) {
            case COM_QUERY: {
                if (pkgType == MySQLPacket.OK_PACKET && !moreResultSets && !preparedOkPacket) {
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
                if (preparedOkPacket) {
                    this.responseState = PacketState.PREPARED;
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
                if (pkgType == RowDataPacket.OK_PACKET && !moreResultSets) {//@todo check this moreResultSets
                    this.responseState = PacketState.RESULT_OK;
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_SET_SECOND_EOF, pkgType, this.responseState);
                    return true;
                }
                if (pkgType == RowDataPacket.ERROR_PACKET) {
                    this.responseState = PacketState.RESULT_ERR;
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_SET_SECOND_EOF, pkgType, this.responseState);
                    return true;
                }
            case RESULT_OK: {
                if (pkgType == RowDataPacket.OK_PACKET && !moreResultSets) {//@todo check this moreResultSets
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_OK, pkgType, this.responseState);
                    return true;
                }
                if (pkgType == RowDataPacket.ERROR_PACKET) {
                    this.responseState = PacketState.RESULT_ERR;
                    logger.debug("from {} meet {} to {} ", PacketState.RESULT_OK, pkgType, this.responseState);
                    return true;
                }
                return false;
            }
            case PREPARED: {
                if (prepareFieldNum > 0) {
                    prepareFieldNum--;
                    return false;
                } else if (prepareFieldNum == 0 && pkgType == EOFPacket.EOF_PACKET) {
                    prepareFieldNum = -1;
                } else if (prepareParamNum > 0) {
                    prepareParamNum--;
                    return false;
                }
                if (prepareParamNum == 0 && pkgType == EOFPacket.EOF_PACKET) {
                    prepareParamNum = -1;
                }
                //but at request prepareFieldNum == -1 && prepareParamNum == -1 is not exist state
                return prepareFieldNum == -1 && prepareParamNum == -1;
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
