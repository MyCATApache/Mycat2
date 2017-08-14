package io.mycat.proxy;

/**
 * 可重用的Buffer，连续读或者写，当空间不够时Compact擦除之前用过的空间，
 * 处于写状态或者读状态之一，不能同时读写，change2Read或change2Write方法来切换读写状态， 只有数据被操作完成（读完或者写完）后State才能被改变
 */
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyBuffer {
	protected static Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);
	private final ByteBuffer buffer;
	
	
	/**
	 * 通道的读写状态标标识，false为写入，true 为读取
	 */
	private boolean inReading = false;
	
	
	/**
	 * 通道读取的状态标识
	 */
	public BufferOptState readState = new BufferOptState();
	
	
	/**
	 * 通道数据写入状态的标识
	 */
	public BufferOptState writeState = new BufferOptState();
	//一般都是后端连接率先给客户端发起信息，所以后端默认使用
	private boolean frontUsing=false;

	public ProxyBuffer(ByteBuffer buffer) {
		super();
		this.buffer = buffer;
		writeState.startPos = 0;
		writeState.optPostion = 0;
		writeState.optLimit = buffer.capacity();
	}

	public boolean isInReading() {
		return inReading;
	}

	public boolean isInWriting() {
		return inReading == false;
	}

	/**
	 * 需要谨慎使用，调用者需要清除当前Buffer所处的状态！！
	 * 
	 * @return ByteBuffer
	 */
	public ByteBuffer getBuffer() {
		return buffer;
	}

	public void setInReading(boolean inReading) {
		this.inReading = inReading;
	}

	public void changeOwner(boolean front)
	{
		this.frontUsing=front;
	}
	/**
	 * 交换Read与Write状态
	 */
	public void flip() {

		if (this.inReading) {
			// 转为可写状态
			inReading = false;
			writeState.startPos = 0;
			writeState.optPostion = 0;
			writeState.optLimit = buffer.capacity();
			writeState.optedTotalLength = 0;
			// 转为可写状态时恢复读状态为初始（不可读）
			readState.startPos = 0;
			readState.optPostion = 0;
			readState.optLimit = 0;
		} else {
			// 转为读状态
			inReading = true;
			//读取状态的开始指针指定为写入状态的开始指针
			readState.startPos = writeState.startPos;
			//opt指针指定为状态开始的指针
			readState.optPostion = writeState.startPos;
			//读取的最大长度指定为现写入的长度
			readState.optLimit = writeState.optPostion;
			//总字节数转换为0
			readState.optedTotalLength = 0;
		}
		logger.debug("flip, new state {} , write state: {} ,read state {}", this.inReading ? "read" : "write",
				this.writeState, this.readState);

	}

	/**
	 * 只能用在读状态下，跳过指定的N个字符
	 * 
	 * @param step
	 */
	public void skip(int step) {
		this.readState.optPostion += step;
	}

	public BufferOptState getReadOptState() {
		return this.readState;
	}

	public BufferOptState getWriteOptState() {
		return this.writeState;
	}

	/**
	 * 写状态时候，如果数据写满了，可以调用此方法移除之前的旧数据
	 */
	public void compact(boolean synReadStatePos) {
		if (this.inReading) {
			throw new RuntimeException("not in writing state ,can't Compact");
		}
		this.buffer.position(writeState.startPos);
		this.buffer.limit(writeState.optPostion);
		this.buffer.compact();
		int offset = writeState.startPos;
		writeState.startPos = 0;
		writeState.optPostion = buffer.limit();
		writeState.optLimit = buffer.capacity();
		buffer.limit(buffer.capacity());
		if (synReadStatePos) {
			readState.optPostion -= offset;
			readState.optLimit -= offset;
		}

	}

	public ProxyBuffer writeBytes(byte[] bytes) {
		this.writeBytes(bytes.length, bytes);
		return this;
	}

	public long readFixInt(int length) {
		long val = getInt(readState.optPostion, length);
		this.readState.optPostion += length;
		return val;
	}

	public long readLenencInt() {
		int index = readState.optPostion;
		long len = getInt(readState.optPostion, 1) & 0xff;
		if (len < 251) {
			this.readState.optPostion += 1;
			return getInt(index, 1);
		} else if (len == 0xfc) {
			this.readState.optPostion += 2;
			return getInt(index + 1, 2);
		} else if (len == 0xfd) {
			this.readState.optPostion += 3;
			return getInt(index + 1, 3);
		} else {
			this.readState.optPostion += 8;
			return getInt(index + 1, 8);
		}
	}

	public long getInt(int index, int length) {
		buffer.limit(index + length);
		buffer.position(index);
		long rv = 0;
		for (int i = 0; i < length; i++) {
			byte b = buffer.get();
			rv |= (((long) b) & 0xFF) << (i * 8);
		}
		return rv;
	}

	public byte[] getBytes(int index, int length) {
		buffer.limit(length + index);
		buffer.position(index);
		byte[] bytes = new byte[length];
		buffer.get(bytes);
		return bytes;
	}

	public byte getByte(int index) {
		buffer.limit(index + 1);
		buffer.position(index);
		byte b = buffer.get();
		return b;
	}

	public String getFixString(int index, int length) {
		byte[] bytes = getBytes(index, length);
		return new String(bytes);
	}

	public String readFixString(int length) {
		byte[] bytes = getBytes(readState.optPostion, length);
		readState.optPostion += length;
		return new String(bytes);
	}

	public String getLenencString(int index) {
		int strLen = (int) getLenencInt(index);
		int lenencLen = getLenencLength(strLen);
		byte[] bytes = getBytes(index + lenencLen, strLen);
		return new String(bytes);
	}

	public String readLenencString() {
		int strLen = (int) getLenencInt(readState.optPostion);
		int lenencLen = getLenencLength(strLen);
		byte[] bytes = getBytes(readState.optPostion + lenencLen, strLen);
		this.readState.optPostion += strLen + lenencLen;
		return new String(bytes);
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
		while (scanIndex < readState.optLimit) {
			if (getByte(scanIndex++) == 0) {
				break;
			}
			strLength++;
		}
		byte[] bytes = getBytes(index, strLength);
		return new String(bytes);
	}

	public String readNULString() {
		String rv = getNULString(readState.optPostion);
		readState.optPostion += rv.getBytes().length + 1;
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
		putFixInt(writeState.optPostion, length, val);
		writeState.optPostion += length;
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
			putByte(writeState.optPostion++, (byte) val);
		} else if (val >= 251 && val < (1 << 16)) {
			putByte(writeState.optPostion++, (byte) 0xfc);
			putFixInt(writeState.optPostion, 2, val);
			writeState.optPostion += 2;
		} else if (val >= (1 << 16) && val < (1 << 24)) {
			putByte(writeState.optPostion++, (byte) 0xfd);
			putFixInt(writeState.optPostion, 3, val);
			writeState.optPostion += 3;
		} else {
			putByte(writeState.optPostion++, (byte) 0xfe);
			putFixInt(writeState.optPostion, 8, val);
			writeState.optPostion += 8;
		}
		return this;
	}

	public ProxyBuffer putFixString(int index, String val) {
		putBytes(index, val.getBytes());
		return this;
	}

	public ProxyBuffer writeFixString(String val) {
		putBytes(writeState.optPostion, val.getBytes());
		writeState.optPostion += val.getBytes().length;
		return this;
	}

	public ProxyBuffer putLenencString(int index, String val) {
		this.putLenencInt(index, val.getBytes().length);
		int lenencLen = getLenencLength(val.getBytes().length);
		this.putFixString(index + lenencLen, val);
		return this;
	}

	public ProxyBuffer writeLenencString(String val) {
		putLenencString(writeState.optPostion, val);
		int lenencLen = getLenencLength(val.getBytes().length);
		writeState.optPostion += lenencLen + val.getBytes().length;
		return this;
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
		buffer.limit(index + length);
		buffer.position(index);
		buffer.put(bytes);
		return this;
	}

	public ProxyBuffer putByte(int index, byte val) {
		buffer.limit(index + 1);
		buffer.position(index);
		buffer.put(val);
		return this;
	}

	public ProxyBuffer putNULString(int index, String val) {
		putFixString(index, val);
		putByte(val.getBytes().length + index, (byte) 0);
		return this;
	}

	public ProxyBuffer writeNULString(String val) {
		putNULString(writeState.optPostion, val);
		writeState.optPostion += val.getBytes().length + 1;
		return this;
	}

	public byte[] readBytes(int length) {
		byte[] bytes = this.getBytes(readState.optPostion, length);
		readState.optPostion += length;
		return bytes;
	}

	public ProxyBuffer writeBytes(int length, byte[] bytes) {
		this.putBytes(writeState.optPostion, length, bytes);
		writeState.optPostion += length;
		return this;
	}

	public ProxyBuffer writeLenencBytes(byte[] bytes) {
		putLenencInt(writeState.optPostion, bytes.length);
		int offset = getLenencLength(bytes.length);
		putBytes(writeState.optPostion + offset, bytes);
		writeState.optPostion += offset + bytes.length;
		return this;
	}

	public ProxyBuffer writeByte(byte val) {
		this.putByte(writeState.optPostion, val);
		writeState.optPostion++;
		return this;
	}

	public byte readByte() {
		byte val = getByte(readState.optPostion);
		readState.optPostion++;
		return val;
	}

	public byte[] getLenencBytes(int index) {
		int len = (int) getLenencInt(index);
		return getBytes(index + getLenencLength(len), len);
	}

	/**
	 * 获取lenenc占用的字节长度
	 *
	 * @param lenenc
	 *            值
	 * @return 长度
	 */
	private int getLenencLength(int lenenc) {
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

	public long getLenencInt(int index) {
		long len = getInt(index, 1) & 0xff;
		if (len == 0xfc) {
			return getInt(index + 1, 2);
		} else if (len == 0xfd) {
			return getInt(index + 1, 3);
		} else if (len == 0xfe) {
			return getInt(index + 1, 8);
		} else {
			return getInt(index, 1);
		}
	}

	public byte[] readLenencBytes() {
		int len = (int) getLenencInt(readState.optPostion);
		byte[] bytes = getBytes(readState.optPostion + getLenencLength(len), len);
		readState.optPostion += getLenencLength(len) + len;
		return bytes;
	}

	public ProxyBuffer putLenencBytes(int index, byte[] bytes) {
		putLenencInt(index, bytes.length);
		int offset = getLenencLength(bytes.length);
		putBytes(index + offset, bytes);
		return this;
	}

	/**
	 * Reset to write状态，清除数据
	 */
	public void reset() {
		inReading = false;
		writeState.optPostion = 0;
		writeState.optLimit = buffer.capacity();
		writeState.curOptedLength = 0;
		writeState.optedTotalLength = 0;
		readState.optPostion = 0;
		readState.optLimit = 0;
		readState.curOptedLength = 0;
		readState.optedTotalLength = 0;
	}

	public boolean frontUsing() {
		return this.frontUsing;
	}
	public boolean backendUsing()
	{
		return !frontUsing;
	}

}