package io.mycat.proxy;
/**
 * Buffer读写时候的状态记录
 * 
 * @author wuzhihui
 *
 */
public class BufferOptState {
	/**
	 * 操作开始时候，数据的起始位置
	 */
	public int startPos;
	/**
	 * 读写数据的当前起始位置
	 */
	public int optPostion;
	/**
	 * 读写数据的长度（byte)
	 */
	public int optLimit;
	
	/**
	 * 读写操作事件中总传输的数据（字节）
	 */
	public int optedTotalLength;
	
	/**
	 * 当前读写事件传输的数据（字节）
	 */
	public int curOptedLength;

	@Override
	public String toString() {
		return "BufferOptState [startPos=" + startPos + ", optPostion=" + optPostion
				+ ", optLimit=" + optLimit + ", optedTotalLength=" + optedTotalLength + "]";
	}

	public boolean hasRemain() {
		return (optPostion<optLimit);
	}
	
	
}

