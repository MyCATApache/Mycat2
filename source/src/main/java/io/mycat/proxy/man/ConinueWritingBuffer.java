package io.mycat.proxy.man;

import java.nio.ByteBuffer;

import io.mycat.proxy.ProxyBuffer;

/**
 * 持续写入的同时也不断发送数据给Socket的Buffer，需要两个状态
 * @author wuzhihui
 *
 */
public class ConinueWritingBuffer extends ProxyBuffer{
    public int readOpt;
    public int readLimit;
	public ConinueWritingBuffer(ByteBuffer buffer) {
		super(buffer);
		 
	}

}
