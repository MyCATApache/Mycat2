package io.mycat.proxy;

/**
 * 可重用的Buffer，连续读或者写，当空间不够时Compact擦除之前用过的空间，
 * 属性state用来表明此Buffer目前所处的状态，即用于写数据（Write to buffer)或者读数据
 * 只有数据被操作完成（读完或者写完）后State才能被改变
 */
import java.nio.ByteBuffer;
public class ProxyBuffer {

    public static enum BufferState {

        READY_TO_WRITE, READY_TO_READ

    }


    private final ByteBuffer buffer;
    
    private BufferState state = BufferState.READY_TO_WRITE;

    public ProxyBuffer(ByteBuffer buffer) {
		super();
		this.buffer = buffer;
	}

	public boolean isReadyToRead() {
        return state == BufferState.READY_TO_READ;
    }

    public boolean isReadyToWrite() {
        return state == BufferState.READY_TO_WRITE;
    }

	public BufferState getState() {
		return state;
	}

	public void setState(BufferState state) {
		this.state = state;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

   

}

