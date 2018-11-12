package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.mycat2.MySQLCommand.COM_STMT_FETCH;
import static io.mycat.mycat2.cmds.judge.MySQLProxyStateM.PacketState.COM_QUERY;

/**
 * cjw
 * 294712221@qq.com
 */
public class MySQLProxyStateM<T> {
    public int serverStatus;
    public boolean isCommandFinished = false;
    public byte commandType;//prepared
    public long prepareFieldNum;
    public long prepareParamNum;
    public MySQLPacketCallback callback;

    public byte getCommandType() {
        return commandType;
    }


    protected static Logger logger = LoggerFactory.getLogger(MySQLProxyStateM.class);

    public MySQLProxyStateM(MySQLPacketCallback callback) {
        this.callback = callback;
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

    public void in(byte commandType) {
        this.commandType = commandType;
        this.responseState = COM_QUERY;
        this.isCommandFinished = false;
        this.prepareFieldNum = 0;
        this.prepareParamNum = 0;
        this.serverStatus = 0;
        switch (commandType) {
            case 25:
                this.isCommandFinished = true;//Request Command Close Statement
            default:

        }
    }

    public boolean isInteractive() {
        return !isCommandFinished || isInteractive(serverStatus)||commandType == COM_STMT_FETCH;
    }

    public static boolean isInteractive(int serverStatus) {
        return JudgeUtil.hasTrans(serverStatus) || JudgeUtil.hasFatch(serverStatus);
    }

    public boolean on(int pkgType) {
        return on(pkgType, serverStatus, false);
    }

    public boolean on(int pkgType, int serverStatus) {
        return on(pkgType, serverStatus, MySQLCommand.COM_STMT_PREPARE == this.commandType);
    }

    public boolean on(int pkgType, int serverStatus, boolean preparedOkPacket) {
        this.serverStatus = serverStatus;
        isCommandFinished = on(pkgType, JudgeUtil.hasMoreResult(serverStatus), JudgeUtil.hasMulitQuery(serverStatus), preparedOkPacket);
        if (isCommandFinished) {
            callback.onCommandFinished(this);
        }
        logger.debug("cmd finished:{}", isCommandFinished);
        return isCommandFinished;
    }

    private boolean on(int pkgType, boolean moreResults, boolean moreResultSets, boolean preparedOkPacket) {
        switch (this.responseState) {
            case COM_QUERY: {//check moreResults in fetch data
                if (pkgType == MySQLPacket.OK_PACKET && !moreResultSets && !moreResults && !preparedOkPacket&&commandType!= COM_STMT_FETCH) {
                    this.responseState = PacketState.RESULT_OK;
                    logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
                    return true;
                } else if (pkgType == MySQLPacket.ERROR_PACKET) {
                    this.responseState = PacketState.RESULT_ERR;
                    logger.debug("from {} meet {} to {} ", COM_QUERY, pkgType, this.responseState);
                    return true;
                }
                if (pkgType == MySQLPacket.EOF_PACKET && !moreResultSets && !moreResults) {
                    if (commandType == MySQLCommand.COM_SET_OPTION ||
                            commandType == MySQLCommand.COM_STMT_EXECUTE||
                            commandType == COM_STMT_FETCH) {
                        return true;
                    }
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
                    callback.onRsFinish(this);
                    if (!moreResults) {
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
                callback.onRsRow(this);
                return false;
            }
            case RESULT_SET_SECOND_EOF:
                if (pkgType == RowDataPacket.OK_PACKET && !moreResultSets&&!moreResults) {//@todo check this moreResultSets
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
        return this.isCommandFinished ||MySQLCommand.COM_STMT_CLOSE == commandType;
    }

    public boolean isRowData() {
        return this.responseState == PacketState.RESULT_SET_FIRST_EOF;
    }

}
