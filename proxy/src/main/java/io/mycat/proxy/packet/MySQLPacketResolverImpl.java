package io.mycat.proxy.packet;

import io.mycat.beans.mysql.MySQLCapabilityFlags;
import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.session.AbstractMySQLSession;

import java.io.IOException;

public class MySQLPacketResolverImpl implements MySQLPacketResolver {
    MySQLPacket mySQLPacket;
    int packetId = 0;
    int head;
    int remainsBytes;
    int startPos;
    int endPos;
    int currentSqlType;
    int length;
    boolean hasResolvePayloadType;
    boolean multiPacket;
    boolean requestFinished;
    boolean isPayloadFinished;
    ComQueryState state = ComQueryState.QUERY_PACKET;
    MySQLPayloadType payloadType;
    long paramId;
    long statementId;
    int columnCount;
    int errorStage;
    int errorMaxStage;
    int errorProgress;
    byte[] errorProgressInfo;
    byte errorMark;
    byte[] errorSqlState;
    byte[] message;
    int prepareOkParametersCount;
    int warningCount;
    int affectedRows;
    int lastInsertId;
    int serverStatus;
    byte[] okStatusInfo;
    byte okSessionStateInfoType;
    byte[] okSessionStateInfoTypeData;
    final boolean CLIENT_DEPRECATE_EOF;
    int wholePacketStartPos;
    int wholePacketEndPos;
    final MySQLCapabilityFlags capabilityFlags;
    final AbstractMySQLSession session;
    MySQLPacketProcessType mySQLPacketProcessType;

    public MySQLPacketResolverImpl(boolean CLIENT_DEPRECATE_EOF, MySQLCapabilityFlags capabilityFlags, AbstractMySQLSession session) {
        this.CLIENT_DEPRECATE_EOF = CLIENT_DEPRECATE_EOF;
        this.capabilityFlags = capabilityFlags;
        this.session = session;
    }

    @Override
    public MySQLPacketProcessType getMySQLPacketProcessType() {
        return mySQLPacketProcessType;
    }

    @Override
    public void setMySQLPacketProcessType(MySQLPacketProcessType type) {
        this.mySQLPacketProcessType = type;
    }

    @Override
    public void setWholePacketStartPos(int length) {
        wholePacketStartPos = length;
    }

    @Override
    public void setWholePacketEndPos(int length) {
        wholePacketEndPos = length;
    }

    @Override
    public int getWholePacketStartPos() {
        return wholePacketStartPos;
    }

    @Override
    public int getWholePacketEndPos() {
        return wholePacketEndPos;
    }


    @Override
    public void setPayloadLength(int length) {
        this.length = length;
    }

    @Override
    public int getPayloadLength() {
        return length;
    }


    @Override
    public int setPacketId(int packetId) {
        return this.packetId = packetId;
    }

    @Override
    public int getPacketId() {
        return this.packetId;
    }

    @Override
    public boolean readFromChannel() throws IOException {
        return session.readFromChannel();
    }

    @Override
    public void writeToChannel() throws IOException {
        session.writeToChannel();
    }


    @Override
    public boolean hasResolvePayloadType() {
        return hasResolvePayloadType;
    }

    @Override
    public void markedResolvePayloadType(boolean marked) {
        hasResolvePayloadType = marked;
    }

    @Override
    public int getHead() {
        return this.head;
    }

    @Override
    public int setHead(int head) {
        return this.head = head;
    }

    @Override
    public int setCurrentComQuerySQLType(int type) {
        return this.currentSqlType = type;
    }

    @Override
    public int getCurrentSQLType() {
        return this.currentSqlType;
    }

    @Override
    public int setStartPos(int i) {
        System.out.println("startPos:" + i);
        return startPos = i;
    }

    @Override
    public int getStartPos() {
        return startPos;
    }

    @Override
    public int setEndPos(int i) {
        System.out.println("endPos:" + i);
        return endPos = i;
    }

    @Override
    public int getEndPos() {
        if (endPos < 0) {
            throw new MycatExpection("");
        }
        return endPos;
    }

    @Override
    public int setPayloadStartPos(int i) {
        return 0;
    }

    @Override
    public int getPayloadStartPos() {
        return 0;
    }

    @Override
    public int setPayloadEndPos(int i) {
        return 0;
    }

    @Override
    public int getPayloadEndPos() {
        return 0;
    }

    @Override
    public ComQueryState getState() {
        return this.state;
    }

    @Override
    public void setState(ComQueryState state) {
        this.state = state;
    }

    @Override
    public void setMySQLpayloadType(MySQLPayloadType type) {
        System.out.println(type);
        this.payloadType = type;
    }

    @Override
    public int setColumnCount(int count) {
        return columnCount = count;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public boolean clientDeprecateEof() {
        return CLIENT_DEPRECATE_EOF;
    }

    @Override
    public MySQLCapabilityFlags capabilityFlags() {
        return capabilityFlags;
    }

    @Override
    public int setRemainsBytes(int remainsBytes) {
        if (remainsBytes < 0) {
            throw new MycatExpection("");
        }
        return this.remainsBytes = remainsBytes;
    }

    @Override
    public int getRemainsBytes() {
        return this.remainsBytes;
    }

    @Override
    public boolean setMultiPacket(boolean c) {
        return multiPacket = c;
    }


    @Override
    public boolean isMultiPacket() {
        return multiPacket;
    }

    @Override
    public MySQLPacket currentProxybuffer() {
        return (MySQLPacket) session.currentProxyBuffer();
    }


    @Override
    public void setRequestFininshed(boolean b) {
        this.requestFinished = b;
    }

    @Override
    public void resetCurrentPayloadMySQLPacket() {
        if (mySQLPacket != null) {
            mySQLPacket.reset();
        }
        mySQLPacket = null;
    }


    @Override
    public boolean isPayloadFinished() {
        return isPayloadFinished;
    }

    @Override
    public boolean setPayloadFinished(boolean b) {
        return isPayloadFinished = b;
    }

    @Override
    public MySQLPayloadType getPailoadType() {
        return payloadType;
    }

    @Override
    public int getErrorStage() {
        return errorStage;
    }

    @Override
    public void setErrorStage(int stage) {
        this.errorStage = stage;
    }

    @Override
    public int getErrorMaxStage() {
        return errorMaxStage;
    }

    @Override
    public void setErrorMaxStage(int maxStage) {
        this.errorMaxStage = maxStage;
    }

    @Override
    public int getErrorProgress() {
        return errorProgress;
    }

    @Override
    public void setErrorProgress(int progress) {
        this.errorProgress = progress;
    }

    @Override
    public byte[] getErrorProgressInfo() {
        return errorProgressInfo;
    }

    @Override
    public void setErrorProgressInfo(byte[] progressInfo) {
        this.errorProgressInfo = progressInfo;
    }

    @Override
    public byte getErrorMark() {
        return errorMark;
    }

    @Override
    public void setErrorMark(byte mark) {
        this.errorMark = mark;
    }

    @Override
    public byte[] getErrorSqlState() {
        return errorSqlState;
    }

    @Override
    public void setErrorSqlState(byte[] sqlState) {
        this.errorSqlState = sqlState;
    }

    @Override
    public byte[] getErrorMessage() {
        return message;
    }

    @Override
    public void setErrorMessage(byte[] message) {
        this.message = message;
    }


    @Override
    public long getPreparedOkStatementId() {
        return this.statementId;
    }

    @Override
    public void setPreparedOkStatementId(long statementId) {
        this.statementId = statementId;
    }

    @Override
    public int getPrepareOkColumnsCount() {
        return columnCount;
    }

    @Override
    public void setPrepareOkColumnsCount(int columnsNumber) {
        this.columnCount = columnsNumber;
    }

    @Override
    public int getPrepareOkParametersCount() {
        return this.prepareOkParametersCount;
    }

    @Override
    public void setPrepareOkParametersCount(int parametersNumber) {
        this.prepareOkParametersCount = parametersNumber;
    }

    @Override
    public int getPreparedOkWarningCount() {
        return warningCount;
    }

    @Override
    public void setPreparedOkWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }


    @Override
    public int getOkAffectedRows() {
        return this.affectedRows;
    }

    @Override
    public void setOkAffectedRows(int affectedRows) {
        this.affectedRows = affectedRows;
    }

    @Override
    public int getOkLastInsertId() {
        return this.lastInsertId;
    }

    @Override
    public void setOkLastInsertId(int lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    @Override
    public int getOkServerStatus() {
        return serverStatus;
    }

    @Override
    public int setOkServerStatus(int serverStatus) {
        return this.serverStatus = serverStatus;
    }

    @Override
    public int getOkWarningCount() {
        return warningCount;
    }

    @Override
    public void setOkWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    @Override
    public byte[] getOkStatusInfo() {
        return okStatusInfo;
    }

    @Override
    public void setOkStatusInfo(byte[] statusInfo) {
        this.okStatusInfo = statusInfo;
    }

    @Override
    public byte getOkSessionStateInfoType() {
        return okSessionStateInfoType;
    }

    @Override
    public void setOkSessionStateInfoType(byte sessionStateInfoType) {
        this.okSessionStateInfoType = sessionStateInfoType;
    }

    @Override
    public byte[] getOkSessionStateInfoTypeData() {
        return this.okSessionStateInfoTypeData;
    }

    @Override
    public void setOkSessionStateInfoTypeData(byte[] sessionStateInfoTypeData) {
        this.okSessionStateInfoTypeData = sessionStateInfoTypeData;
    }

    @Override
    public byte[] getOkMessage() {
        return message;
    }

    @Override
    public void setOkMessage(byte[] message) {
        this.message = message;
    }

    @Override
    public int getEofWarningCount() {
        return this.warningCount;
    }

    @Override
    public void setEofWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    @Override
    public int getEofServerStatus() {
        return this.serverStatus;
    }

    @Override
    public int setEofServerStatus(int status) {
        return this.serverStatus = status;
    }


    public void setCurrentPayloadMySQLPacket(MySQLPacket mySQLPacket) {
        this.mySQLPacket = mySQLPacket;
    }


}
