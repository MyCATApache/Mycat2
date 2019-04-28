package io.mycat.proxy.packet;

public interface PreparedOKPacket {

    public long getPreparedOkStatementId();

    public void setPreparedOkStatementId(long statementId);

    public int getPrepareOkColumnsCount();

    public void setPrepareOkColumnsCount(int columnsNumber);

    public int getPrepareOkParametersCount();

    public void setPrepareOkParametersCount(int parametersNumber);

    public int getPreparedOkWarningCount();

    public void setPreparedOkWarningCount(int warningCount);
}
