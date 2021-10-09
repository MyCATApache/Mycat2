package io.mycat.swapbuffer;

//已经写入的数据区域
public interface PacketResponse {
     PacketRequest getRequest();
     int getCopyCount();
     void setCopyCount(int n);
}
