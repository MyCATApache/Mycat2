package io.mycat.proxy;

import java.io.IOException;
/**
 * 可重用的Buffer，连续读或者写，当空间不够时Compact擦除之前用过的空间，
 * 处于写状态或者读状态之一，不能同时读写，change2Read或change2Write方法来切换读写状态， 只有数据被操作完成（读完或者写完）后State才能被改变
 */
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


import org.slf4j.*;

public class ProxyBuffer {
	protected static Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);
	private final ByteBuffer buffer;

	private boolean inReading = false;;
    private BufferOptState readState=new BufferOptState();
    private BufferOptState writeState=new BufferOptState();
	public ProxyBuffer(ByteBuffer buffer) {
		super();
		this.buffer = buffer;
		writeState.startPos=0;
		writeState.optPostion=0;
		writeState.optLimit=buffer.capacity();
	}

	public boolean isInReading() {
		return inReading;
	}

	public boolean isInWriting() {
		return inReading == false;
	}

	/**
	 * 需要谨慎使用，调用者需要清除当前Buffer所处的状态！！
	 * @return ByteBuffer
	 */
	public ByteBuffer getBuffer() {
		return buffer;
	}


	/**
	 * 改为写状态，需要提供正确的写入位置
	 * 
	 * @param writeState
	 */
	public void change2Write(int writeStart,int writeLimit) {
		this.inReading=false;
		this.writeState.optPostion=writeStart;
		this.writeState.optLimit=writeLimit;
		
	}
	/**
	 * 改为读状态
	 * 
	 * @param readState，需要提供正确的数据读取位置
	 */
	public void change2Read(int readStart,int readLimit) {
		this.inReading=true;
		this.readState.optPostion=readStart;
		this.readState.optLimit=readLimit;
	}
	/**
	 * 交换Read与Write状态
	 */
	public void flip()
	{
		//logger.info("flip, is reading ?"+this.inReading+" write state:"+this.writeState+" ,read state "+this.readState);
		if(this.inReading)
		{
			inReading=false;
			writeState.startPos=0;
			writeState.optPostion=0;
			writeState.optLimit=buffer.capacity();
			writeState.optedTotalLength=0;
		}else
		{
			inReading=true;
			readState.startPos=writeState.startPos;
			readState.optPostion=writeState.startPos;
			readState.optLimit=writeState.startPos+writeState.optedTotalLength;
			readState.optedTotalLength=0;
		}
	}
	/**
	 * 从SocketChannel中读取数据并写入到内部Buffer中, 第一次调用之前需要确保Buffer状态为Write状态，并指定要写入的位置，参考change2write() 方法，
	 * 
	 * @param channel
	 * @return 读取了多少数据
	 */
	public int writeFromChannel(SocketChannel channel) throws IOException {
		buffer.position(this.writeState.optPostion);
		buffer.limit(this.writeState.optLimit); 
	    int readed=channel.read(buffer);
	    writeState.curOptedLength=readed;
	    if(readed>0)
	    {
	    	writeState.optPostion+=readed;
	    	writeState.optedTotalLength+=readed;
	    }
		return readed;
	}
	/**
	 * 从内部Buffer数据写入到SocketChannel中发送出去，第一次调用之前需要确保Buffer状态为Read状态，并指定读取的位置，参考change2read() 方法，
	 * 
	 * @param channel
	 * @return 是否写完了Buffer中的内容
	 */
	public boolean readToChannel(SocketChannel channel) throws IOException {
		buffer.position(this.readState.optPostion);
		buffer.limit(this.readState.optLimit); 
	    int writed=channel.write(buffer);
	    readState.curOptedLength=writed;
	    readState.optPostion+=writed;
	    readState.optedTotalLength+=writed;
	    if(buffer.remaining()==0)
	    {
	    	return true;
	    }else
	    {
	    	return false;
	    }
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
	public void compact() {
		if(this.inReading)
		{
			throw new RuntimeException("not in writing state ,can't Compact");
		}
		this.buffer.position(writeState.startPos);
		this.buffer.limit(writeState.optPostion);
		this.buffer.compact();
		writeState.startPos=0;
		writeState.optPostion=buffer.limit();
		writeState.optLimit=buffer.capacity();
		this.buffer.limit(buffer.capacity());
		
	}

}