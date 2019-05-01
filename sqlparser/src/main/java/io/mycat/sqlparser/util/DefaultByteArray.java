package io.mycat.sqlparser.util;

/**
 * Created by jamie on 2017/8/29.
 */
public class DefaultByteArray implements ByteArrayView {
    byte[] src;
    int offset = 0;
    @Override
    public byte getByte(int index) {
        return src[index];
    }

    @Override
    public int length() {
        return src.length;
    }

    @Override
    public void set(int index, byte value) {
        src[index]=value;
    }

    @Override
    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public int getOffset() {
        return this.offset;
    }

    public byte[] getSrc() {
        return src;
    }

    public void setSrc(byte[] src) {
        this.src = src;
    }

    public DefaultByteArray(byte[] src) {
        this.src = src;
    }

    public DefaultByteArray() {
    }
}
