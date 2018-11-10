package io.mycat.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * <pre>
 * 可重用的Buffer，连续读或者写，当空间不够时Compact擦除之前用过的空间， 处于写状态或者读状态之一，不能同时读写，
 * 只有数据被操作完成（读完或者写完）后State才能被改变（flip方法或手工切换状态），同时可能要改变Owner，chanageOwn
 *
 * 需要外部关心的状态
 * writeIndex 写入buffer开始位置;
 * readIndex 读取开始位置;
 * inReading 当前buffer读写状态，inReading=true表示读状态，inReading=false表示写状态。
 * 不需要外部关心的状态为
 * readMark 向channel 中写入数据时的开始位置, 该状态由writeToChannel 自动维护,不需要外部显式指定;
 *
 * 使用流程
 * 一、透传、只前端读写、只后端读写场景
 * 1. 从channel 向 buffer 写入数据
 * 始终从 writeIndex 开始写入 , inReading状态一定为 false 写入状态
 * 2. 读取 buffer 中数据
 * 读取的数据范围是 readIndex --- writeIndex 之间的数据.
 * 3. 向 channel 写入数据前, flip 切换读写状态
 * 4. 数据全部透传完成（例如:整个结果集透传完成）后 changeOwner,否则 owner不变.
 * 5. 从 buffer 向 channel 写入数据时,写入 readMark--readIndex 之间的数据.
 * 6. 写完成后 flip切换读写状态。同时 如果 readIndex > buffer.capacity() * 2 / 3 进行一次压缩
 * 7. 从 channel向buffer 写入数据时，如果 writeIndex > buffer.capacity() * 1 / 3 进行一次压缩
 *
 * 二、没有读取数据,向buffer中写入数据后 直接 writePayload 到 channel的场景
 * 1. 在写入到 channel 时 ,需要显式 指定readIndex = writeIndex;
 * 2. 其他步骤 同 （透传、只前端读写、只后端读写场景）场景
 * </pre>
 *
 * @author yanjunli
 */
public class ProxyBuffer {

    protected static Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);
    /*
     * 写入数据开始位置， 从socket buffer 写入数据时,从该位置开始写入
     */
    public int writeIndex;

    // 对于Write to Buffer
    // 的操作，readIndex表示当前可读的数据截止位置，readMark为数据开始位置，用户可以标记，下次写入Buffer时，从readMark的位置继续写入，
    //写入操作执行时，readMark会累加，当readMark==readIndex，表示指定的数据已经全部写入到Channel中，即无可写内容

    // 对于Read to
    // Channel的操作，readMark表示读取数据截止的位置，writeIndex表示数据截止的位置，0-readMark之间的数据是要被写入CHannel的，写入Channel
    // 后，Write 完成后检查optMark；当writeIndex > 1/3 capacity 时 执行 compact。将readIndex 与
    // writeIndex 之间的数据移动到buffer 开始位置
    /*
     * 用于标记透传时 的结束位置 向socket 中吸入数据时,用于标记写入到socket 中数据的结束位置
     */
    public int readIndex;
    /*
     * 用于标记透传时数据开始位置, 这个指针不推荐外部用户 显示调用，由网络读写状态自动管理 为减少 compact 次数. 引入第三个指针. 在
     * writeIndex > 2/3 capacity 时 执行 compact
     */
    public int readMark;
    private ByteBuffer buffer;

    /*
     * ********************************************************************* 以下
     * 两个状态 ,不推荐 外部使用. 会在进行网络读写时,网络读写程序内部使用.
     * *********************************************************************
     */
    /**
     * 当前buffer 的读写状态 . 默认为写入状态 false 从 channel 中 向 buffer 写入数据 即： writePayload 状态 true
     * 从buffer 向 channel 写出数据 即： readPayload 状态
     */
    private boolean inReading = false;

    public ProxyBuffer(ByteBuffer buffer) {
        super();
        this.buffer = buffer;
    }

    /**
     * 获取lenenc占用的字节长度
     *
     * @param lenenc 值
     * @return 长度
     */
    public static int getLenencLength(int lenenc) {
        if (lenenc < 251) {
            return 1;
        } else if (lenenc >= 251 && lenenc < (1 << 16)) {
            return 3;
        } else if (lenenc >= (1 << 16) && lenenc < (1 << 24)) {
            return 4;
        } else {
            return 9;
        }
    }

    public static int getLenencLength(byte[] bytes) {
        int size = bytes.length;
        return size + getLenencLength(size);
    }

    public boolean isInReading() {
        return inReading;
    }

    public boolean isInWriting() {
        return inReading == false;
    }

    /**
     * 写数据到Socket中时，是否写完了预先指定的Buffer内容
     *
     * @return
     */
    public boolean writeFinished() {

        //buffer.limit(proxyBuffer.readIndex);
        //buffer.position(proxyBuffer.readMark);
        return readIndex == readMark;
    }
    public boolean readFinished() {

        //buffer.limit(proxyBuffer.readIndex);
        //buffer.position(proxyBuffer.readMark);
        return readIndex == writeIndex;
    }
    /**
     * 需要谨慎使用，调用者需要清除当前Buffer所处的状态！！
     *
     * @return ByteBuffer
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer newBuffer) {
        this.buffer = newBuffer;
    }

    /**
     * 重置Buffer，所有Index归零，Buffer清空，并且设置为writing状态
     */
    public void reset() {
        this.inReading = false;
        this.readMark = 0;
        this.readIndex = 0;
        this.writeIndex = 0;
        this.buffer.clear();
    }

    public void resetBuffer(ByteBuffer newBuffer) {
        this.readIndex = 0;
        this.readMark = 0;
        this.writeIndex = newBuffer.position();
        this.buffer = newBuffer;
    }

    /**
     * 只能用在读状态下，跳过指定的N个字符
     *
     * @param step
     */
    public void skip(int step) {
        readIndex += step;
    }

    /**
     * 写状态时候，如果数据写满了，可以调用此方法移除之前的旧数据
     */
    public void compact() {

        this.buffer.position(readMark);
        this.buffer.limit(writeIndex);
        this.buffer.compact();
        readIndex -= readMark;
        readMark = 0;
        writeIndex = buffer.position();
    }

    /**
     * 交换Read与Write状态
     */
    public void flip() {
        this.inReading = !this.inReading;
    }

    public ProxyBuffer writeBytes(byte[] bytes) {
        this.writeBytes(bytes.length, bytes);
        return this;
    }

    public long readFixInt(int length) {
        long val = getInt(readIndex, length);
        readIndex += length;
        return val;
    }

    public long getFixInt(int index, int length) {
        return getInt(index, length);
    }

    public long readLenencInt() {
        int index = readIndex;
        long len = getInt(index, 1) & 0xff;
        if (len < 251) {
            readIndex += 1;
            return getInt(index, 1);
        } else if (len == 0xfc) {
            readIndex += 3;
            return getInt(index + 1, 2);
        } else if (len == 0xfd) {
            readIndex += 4;
            return getInt(index + 1, 3);
        } else {
            readIndex += 9;
            return getInt(index + 1, 8);
        }
    }

    public long getInt(int index, int length) {
        buffer.position(index);
        long rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = buffer.get();
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }

    public byte[] getBytes(int index, int length) {
        buffer.position(index);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    public byte getByte(int index) {
        buffer.position(index);
        byte b = buffer.get();
        return b;
    }

    public String getFixString(int index, int length) {
        byte[] bytes = getBytes(index, length);
        return new String(bytes);
    }

    public String readFixString(int length) {
        byte[] bytes = getBytes(readIndex, length);
        readIndex += length;
        return new String(bytes);
    }

    public String getLenencString(int index) {
        int strLen = (int) getLenencInt(index);
        int lenencLen = getLenencLength(strLen);
        byte[] bytes = getBytes(index + lenencLen, strLen);
        return new String(bytes);
    }

    public String readLenencString() {
        int strLen = (int) getLenencInt(readIndex);
        int lenencLen = getLenencLength(strLen);
        byte[] bytes = getBytes(readIndex + lenencLen, strLen);
        readIndex += strLen + lenencLen;
        return new String(bytes);
    }

    public byte[] readLenencStringBytes() {
        int strLen = (int) getLenencInt(readIndex);
        int lenencLen = getLenencLength(strLen);
        byte[] bytes = getBytes(readIndex + lenencLen, strLen);
        readIndex += strLen + lenencLen;
        return bytes;
    }

    public String getVarString(int index, int length) {
        return getFixString(index, length);
    }

    public String readVarString(int length) {
        return readFixString(length);
    }

    public String getNULString(int index) {
        int strLength = 0;
        int scanIndex = index;
        while (scanIndex < writeIndex) {
            if (getByte(scanIndex++) == 0) {
                break;
            }
            strLength++;
        }
        byte[] bytes = getBytes(index, strLength);
        return new String(bytes);
    }

    public String readNULString() {
        String rv = getNULString(readIndex);
        readIndex += rv.getBytes().length + 1;
        return rv;
    }

    public ProxyBuffer putFixInt(int index, int length, long val) {
        int index0 = index;
        for (int i = 0; i < length; i++) {
            byte b = (byte) ((val >> (i * 8)) & 0xFF);
            putByte(index0++, b);
        }
        return this;
    }

    public ProxyBuffer writeFixInt(int length, long val) {
        putFixInt(writeIndex, length, val);
        writeIndex += length;
        return this;
    }

    public ProxyBuffer putLenencInt(int index, long val) {
        if (val < 251) {
            putByte(index, (byte) val);
        } else if (val >= 251 && val < (1 << 16)) {
            putByte(index, (byte) 0xfc);
            putFixInt(index + 1, 2, val);
        } else if (val >= (1 << 16) && val < (1 << 24)) {
            putByte(index, (byte) 0xfd);
            putFixInt(index + 1, 3, val);
        } else {
            putByte(index, (byte) 0xfe);
            putFixInt(index + 1, 8, val);
        }
        return this;
    }

    public ProxyBuffer writeLenencInt(long val) {
        if (val < 251) {
            putByte(writeIndex++, (byte) val);
        } else if (val >= 251 && val < (1 << 16)) {
            putByte(writeIndex++, (byte) 0xfc);
            putFixInt(writeIndex, 2, val);
            writeIndex += 2;
        } else if (val >= (1 << 16) && val < (1 << 24)) {
            putByte(writeIndex++, (byte) 0xfd);
            putFixInt(writeIndex, 3, val);
            writeIndex += 3;
        } else {
            putByte(writeIndex++, (byte) 0xfe);
            putFixInt(writeIndex, 8, val);
            writeIndex += 8;
        }
        return this;
    }

    public ProxyBuffer putFixString(int index, String val) {
        putBytes(index, val.getBytes());
        return this;
    }

    public ProxyBuffer putFixString(int index, byte[] val) {
        putBytes(index, val);
        return this;
    }

    public ProxyBuffer writeFixString(String val) {
        byte[] bytes = val.getBytes();
        putBytes(writeIndex, bytes);
        writeIndex += bytes.length;
        return this;
    }

    public ProxyBuffer writeFixString(byte[] val) {
        putBytes(writeIndex, val);
        writeIndex += val.length;
        return this;
    }

    public ProxyBuffer putLenencString(int index, String val) {
        byte[] bytes = val.getBytes();
        this.putLenencInt(index, bytes.length);
        int lenencLen = getLenencLength(bytes.length);
        this.putFixString(index + lenencLen, bytes);
        return this;
    }

    public ProxyBuffer putLenencString(int index, byte[] val) {
        this.putLenencInt(index, val.length);
        int lenencLen = getLenencLength(val.length);
        this.putFixString(index + lenencLen, val);
        return this;
    }

    public ProxyBuffer writeLenencString(byte[] bytes) {
        putLenencString(writeIndex, bytes);
        int lenencLen = getLenencLength(bytes.length);
        writeIndex += lenencLen + bytes.length;
        return this;
    }

    public ProxyBuffer writeLenencString(String val) {
        return writeLenencString(val.getBytes());
    }

    public ProxyBuffer putVarString(int index, String val) {
        putFixString(index, val);
        return this;
    }

    public ProxyBuffer writeVarString(String val) {
        return writeFixString(val);
    }

    public ProxyBuffer putBytes(int index, byte[] bytes) {
        putBytes(index, bytes.length, bytes);
        return this;
    }

    public ProxyBuffer putBytes(int index, int length, byte[] bytes) {
        buffer.position(index);
        buffer.put(bytes, 0, length);
        return this;
    }

    public ProxyBuffer putByte(int index, byte val) {
        buffer.position(index);
        buffer.put(val);
        return this;
    }

    public ProxyBuffer putNULString(int index, String val) {
        byte[] bytes = val.getBytes();
        putFixString(index, bytes);
        putByte(bytes.length + index, (byte) 0);
        return this;
    }

    public ProxyBuffer putNULString(int index, byte[] bytes) {
        putFixString(index, bytes);
        putByte(bytes.length + index, (byte) 0);
        return this;
    }

    public ProxyBuffer writeNULString(String val) {
        byte[] bytes = val.getBytes();
        putNULString(writeIndex, bytes);
        writeIndex += bytes.length + 1;
        return this;
    }

    public byte[] readBytes(int length) {
        byte[] bytes = this.getBytes(readIndex, length);
        readIndex += length;
        return bytes;
    }

    public ProxyBuffer writeBytes(int length, byte[] bytes) {
        this.putBytes(writeIndex, length, bytes);
        writeIndex += length;
        return this;
    }

    public ProxyBuffer writeLenencBytes(byte[] bytes) {
        putLenencInt(writeIndex, bytes.length);
        int offset = getLenencLength(bytes.length);
        putBytes(writeIndex + offset, bytes);
        writeIndex += offset + bytes.length;
        return this;
    }

    public ProxyBuffer writeLenencBytes(byte[] bytes, byte[] nullValue) {
        if (bytes == null) {
            return writeLenencBytes(nullValue);
        } else {
            return writeLenencBytes(bytes);
        }
    }

    public ProxyBuffer writeByte(byte val) {
        this.putByte(writeIndex, val);
        writeIndex++;
        return this;
    }
    public ProxyBuffer writeReserved(int length) {
        for (int i = 0; i < length; i++) {
            this.putByte(writeIndex, (byte) 0);
            writeIndex++;
        }
        return this;
    }
    public byte readByte() {
        byte val = getByte(readIndex);
        readIndex++;
        return val;
    }

    public byte[] getLenencBytes(int index) {
        int len = (int) getLenencInt(index);
        return getBytes(index + getLenencLength(len), len);
    }

    public long getLenencInt(int index) {
        long len = getInt(index, 1) & 0xff;
        if (len == 0xfc) {
            return getInt(index + 1, 2);
        } else if (len == 0xfd) {
            return getInt(index + 1, 3);
        } else if (len == 0xfe) {
            return getInt(index + 1, 8);
        } else if (len == 0xfb) {
            return len;
        } else {
            return len;
        }
    }

    public byte[] readLenencBytes() {
        int len = (int) getLenencInt(readIndex);
        byte[] bytes = null;
        if ((len & 0xff) == 0xfb) {
            bytes = null;
        } else {
            bytes = getBytes(readIndex + getLenencLength(len), len);
        }
        readIndex += getLenencLength(len) + len;
        return bytes;
    }

    public ProxyBuffer putLenencBytes(int index, byte[] bytes) {
        putLenencInt(index, bytes.length);
        int offset = getLenencLength(bytes.length);
        putBytes(index + offset, bytes);
        return this;
    }

    @Override
    public String toString() {
        return "ProxyBuffer [buffer=" + buffer + ", writeIndex=" + writeIndex + ", readIndex=" + readIndex
                + ", readMark=" + readMark + ", inReading=" + inReading + "]";
    }
}
