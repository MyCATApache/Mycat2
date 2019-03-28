package io.mycat.mycat2.sqlparser.byteArrayInterface;

import java.nio.ByteBuffer;

public class ByteBufferArray implements ByteArrayView {
    ByteBuffer src;
    int offset = 0;
    int length = 0;
    @Override
    public byte get(int index) {
        return src.get(index);
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public void set(int index, byte value) {
    	src.put(index, value);
        return;
    }

    public ByteBuffer getSrc() {
        return src;
    }

    public void setSrc(ByteBuffer src) {
        this.src = src;
    }
    public ByteBufferArray() {

    }
    public ByteBufferArray(byte[] arg) {
        src = ByteBuffer.wrap(arg);
        offset=0;
        length=arg.length;
    }
    public ByteBufferArray(ByteBuffer arg) {
        src = arg;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
    public int getOffset() { return this.offset; }
    public void setLength(int length) { this.length = length; }
}
