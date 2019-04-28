package io.mycat.proxy.packet;

public interface OkPacket{
//    public long affectedRows;
//    public long lastInsertId;
//    public int serverStatus;
//    public int warningCount;
//    public byte[] statusInfo;

    //    public byte sessionStateInfoType;
//    public byte[] sessionStateInfoTypeData;
//    public byte[] message;
    public int getOkAffectedRows();

    public void setOkAffectedRows(int affectedRows);

    public int getOkLastInsertId();

    public void setOkLastInsertId(int lastInsertId);

    public int getOkServerStatus();

    public int setOkServerStatus(int serverStatus);

    public int getOkWarningCount();

    public void setOkWarningCount(int warningCount);

    public byte[] getOkStatusInfo();

    public void setOkStatusInfo(byte[] statusInfo);

    public byte getOkSessionStateInfoType();

    public void setOkSessionStateInfoType(byte sessionStateInfoType);

    public byte[] getOkSessionStateInfoTypeData();

    public void setOkSessionStateInfoTypeData(byte[] sessionStateInfoTypeData);

    public byte[] getOkMessage();

    public void setOkMessage(byte[] message);

}
