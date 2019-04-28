package io.mycat.proxy.packet;

public interface ErrorPacket {
     static final byte SQLSTATE_MARKER = (byte) '#';
     static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    public int getErrorStage() ;

    public void setErrorStage(int stage);

    public int getErrorMaxStage() ;

    public void setErrorMaxStage(int maxStage) ;

    public int getErrorProgress() ;

    public void setErrorProgress(int progress) ;

    public byte[] getErrorProgressInfo() ;
    public void setErrorProgressInfo(byte[] progress_info);

    public byte getErrorMark() ;

    public void setErrorMark(byte mark) ;

    public byte[] getErrorSqlState() ;

    public void setErrorSqlState(byte[] sqlState);

    public byte[] getErrorMessage() ;

    public void setErrorMessage(byte[] message) ;

}
