package io.mycat.proxy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
/**
 * 每个Reactor一个独立的BufferPool
 * @author wuzhihui
 *
 */
public class BufferPool {

	private ArrayList<ByteBuffer> allBuffers=new ArrayList<ByteBuffer>();
	private final int bufferSize;
	public BufferPool(int size)
	{
		bufferSize=size;
	}
	public ByteBuffer allocByteBuffer()
	{
		if(!allBuffers.isEmpty())
		{
			//System.out.println("xxxx reused  ");
			return allBuffers.remove(0);
		}else
		{
			//System.out.println("xxx created  ");
			return ByteBuffer.allocateDirect(bufferSize);
		}
	}
	public void recycleBuf(ByteBuffer buf) {
		//System.out.println("xxx recyced "+buf);
		buf.clear();
		this.allBuffers.add(buf);
		
	}
}
