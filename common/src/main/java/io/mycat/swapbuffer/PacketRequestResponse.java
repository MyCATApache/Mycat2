package io.mycat.swapbuffer;

public abstract class PacketRequestResponse implements PacketRequest,PacketResponse{
    int copyCount;
    int length;
    int offset;
    @Override
    public PacketRequest getRequest() {
        return this;
    }

    @Override
    public int getCopyCount() {
        return copyCount;
    }

    @Override
    public void setCopyCount(int n) {
         copyCount = n;
    }

    @Override
    public PacketResponse response(int copyCount) {
        this.copyCount = copyCount;
        return this;
    }

    @Override
    public PacketResponse response() {
        this.copyCount = this.length();
        return this;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public void close() {
        PacketRequest.super.close();
    }
}
