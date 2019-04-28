package io.mycat.proxy.packet;

public interface EOFPacket {
    public int getEofWarningCount();

    public void setEofWarningCount(int warningCount) ;

    public int getEofServerStatus() ;

    public int setEofServerStatus(int status);
}
