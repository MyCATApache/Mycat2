package io.mycat.beans.mysql.packet;

public class DefaultPreparedOKPacket implements PreparedOKPacket {
    long pstmt;
    int columnCount;
    int parametersCount;
    int warningCount;

    public DefaultPreparedOKPacket(long pstmt, int columnCount, int parametersCount, int warningCount) {
        this.pstmt = pstmt;
        this.columnCount = columnCount;
        this.parametersCount = parametersCount;
        this.warningCount = warningCount;
    }

    @Override
    public long getPreparedOkStatementId() {
        return pstmt;
    }

    @Override
    public void setPreparedOkStatementId(long statementId) {
        this.pstmt = statementId;
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
        return parametersCount;
    }

    @Override
    public void setPrepareOkParametersCount(int parametersNumber) {
        this.parametersCount = parametersNumber;
    }

    @Override
    public int getPreparedOkWarningCount() {
        return warningCount;
    }

    @Override
    public void setPreparedOkWarningCount(int warningCount) {
        this.warningCount =warningCount;

    }
}