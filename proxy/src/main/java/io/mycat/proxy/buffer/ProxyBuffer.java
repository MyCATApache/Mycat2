package io.mycat.proxy.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface ProxyBuffer {
    final static Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);

    ByteBuffer currentByteBuffer();

    int capacity();
    public int position();

    public int position(int index);

    public void writeFloat(float f);

    public float readFloat();

    public void writeLong(long l);

    public long readLong();

    public void writeDouble(double d);

    public double readDouble();

    public byte get();

    public byte get(int index);

    public ProxyBuffer get(byte[] bytes);

    public byte put(byte b);

    public void put(byte[] bytes, int offset, int legnth);

    public int channelWriteStartIndex();

    public int channelWriteEndIndex();

    public int channelReadStartIndex();

    public int channelReadEndIndex();

    public void channelWriteStartIndex(int index);

    public void channelWriteEndIndex(int index);

    public void channelReadStartIndex(int index);

    public void channelReadEndIndex(int index);
    public void expendToLength(int length) ;

    public boolean readFromChannel(SocketChannel channel) throws IOException;

    public void writeToChannel(SocketChannel channel) throws IOException;

    public BufferPool bufferPool();

    public void reset();

    public void newBuffer();

    public void newBuffer(byte[] bytes);

    public void newBuffer(int len);

    //    public void compactInChannelWritingIfNeed();
    void expendToLengthIfNeedInReading(int length);
    public void appendLengthIfInReading(int length);
    public void appendLengthIfInReading(int length, boolean c);
    public void compactInChannelReadingIfNeed();

//    public void compactOrExpendIfNeedRemainsBytesInReading(int len);

    public ProxyBuffer newBufferIfNeed();
//    {
//        if (buffers() == null) {
//            newBuffer();
//        }
//        return this;
//    }

//    public void expend(int len);

//    public void compactOrExpendIfNeedRemainsBytesInWriting(int len);

    public default boolean channelWriteFinished() {
        boolean b = channelWriteStartIndex() == channelWriteEndIndex();
        return b;
    }

    public default boolean channelReadFinished() {
        return channelReadStartIndex() == channelReadEndIndex();
    }

    public ProxyBuffer applyChannelWritingIndex();

    //    {
//        ByteBuffer buffer = buffers();
//        buffer.position(channelWriteStartIndex());
//        buffer.limit(channelWriteEndIndex());
//        return this;
//    }
    void cutRangeBytesInReading(int start, int end);

    public ProxyBuffer applyChannelReadingIndex();
//    {
//        ByteBuffer buffer = buffers();
//        buffer.position(channelReadStartIndex());
//        buffer.limit(channelReadEndIndex());
//        return this;
//    }

    public void applyChannelWritingIndexForChannelReadingIndex();
//    {
//        ByteBuffer buffer = buffers();
//        channelReadStartIndex(channelWriteStartIndex());
//        channelReadEndIndex(channelWriteEndIndex());
//    }
}
