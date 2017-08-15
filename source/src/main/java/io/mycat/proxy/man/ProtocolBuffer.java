package io.mycat.proxy.man;
/**
 * 提供了基本数据类型写入和读取功能的Buffer对象
 * @author wuzhihui
 *
 */

import java.nio.ByteBuffer;

public class ProtocolBuffer {
	private final ByteBuffer buffer;
	// 对于Write to Buffer
	// 的操作，Limit表示当前可读的数据截止位置，optMark为数据开始位置，用户可以标记，下次写入Buffer时，从optLimit的位置继续写入

	// 对于Read to
	// Channel的操作，optMark表示读取数据截止的位置，optLimit表示数据截止的位置，这之间的数据是要被写入CHannel的，写入Channel
	// 后，optMark位置会增加，如果有新数据被追加到Buffer里，则optLimit位置增加，
	public int optLimit;
	public int optMark;

	public ProtocolBuffer(ByteBuffer buffer) {
		super();
		this.buffer = buffer;
		
	}

	public void reset()
	{
		this.optLimit=0;
		this.optMark=0;
		this.buffer.clear();
	}
	public ProtocolBuffer writeByte(byte val) {
		this.putByte(optLimit, val);
		optLimit++;
		return this;
	}

	public ProtocolBuffer writeBytes(byte[] bytes) {
		this.writeBytes(bytes.length, bytes);
		return this;
	}

	public ProtocolBuffer writeBytes(int length, byte[] bytes) {
		this.putBytes(optLimit, length, bytes);
		optLimit += length;
		return this;
	}

	public ProtocolBuffer putBytes(int index, int length, byte[] bytes) {
		buffer.position(index);
		buffer.put(bytes);
		return this;
	}

	public ProtocolBuffer putBytes(int index, byte[] bytes) {
		putBytes(index, bytes.length, bytes);
		return this;
	}

	public ProtocolBuffer putByte(int index, byte val) {
		buffer.position(index);
		buffer.put(val);
		return this;
	}

	public ProtocolBuffer putFixString(int index, String val) {
		putBytes(index, val.getBytes());
		return this;
	}

	public ProtocolBuffer writeFixString(String val) {
		putBytes(optLimit, val.getBytes());
		optLimit += val.getBytes().length;
		return this;
	}

	public ProtocolBuffer putNULString(int index, String val) {
		putFixString(index, val);
		putByte(val.getBytes().length + index, (byte) 0);
		return this;
	}

	public ProtocolBuffer writeNULString(String val) {
		putNULString(optLimit, val);
		optLimit += val.getBytes().length + 1;
		return this;
	}

	public ProtocolBuffer writeFixInt(int length, long val) {
		putFixInt(optLimit, length, val);
		optLimit += length;
		return this;
	}

	public ProtocolBuffer putFixInt(int index, int length, long val) {
		int index0 = index;
		for (int i = 0; i < length; i++) {
			byte b = (byte) ((val >> (i * 8)) & 0xFF);
			putByte(index0++, b);
		}
		return this;
	}

	/**
	 * 只能用在读操作下，跳过指定的N个字符
	 * 
	 * @param step
	 */
	public void skip(int step) {
		this.optLimit += step;
	}

	public byte[] readBytes(int length) {
		byte[] bytes = getBytes(optLimit, length);
		optLimit += length;
		return bytes;
	}

	public byte readByte() {
		byte val = getByte(optLimit);
		optLimit++;
		return val;
	}

	public byte getByte(int index) {
		buffer.position(index);
		byte b = buffer.get();
		return b;
	}

	public long readFixInt(int length) {
		long val = getInt(optLimit, length);
		optLimit += length;
		return val;
	}

	public String readNULString() {
		String rv = getNULString(optLimit);
		optLimit += rv.getBytes().length + 1;
		return rv;
	}

	public String getNULString(int index) {
		int strLength = 0;
		int scanIndex = index;
		while (scanIndex < buffer.limit()) {
			if (getByte(scanIndex++) == 0) {
				break;
			}
			strLength++;
		}
		byte[] bytes = getBytes(index, strLength);
		return new String(bytes);
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

	public ByteBuffer getBuffer() {
		return buffer;
	}

}
