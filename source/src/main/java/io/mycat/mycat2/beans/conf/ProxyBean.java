package io.mycat.mycat2.beans.conf;

import io.mycat.mycat2.beans.GlobalBean;
import io.mycat.proxy.buffer.BufferPooLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Desc: mycat代理配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class ProxyBean {
	
    private static final short DEFAULT_BUFFER_CHUNK_SIZE = 1024*4*2;
    private static final int DEFAULT_BUFFER_POOL_PAGE_SIZE = 1024*1024*4;
    private static final short DEFAULT_BUFFER_POOL_PAGE_NUMBER = 64;
	private static final Logger logger = LoggerFactory.getLogger(ProxyBean.class);
    private static final int MAX_ALLOWED_PACKET  = 16 * 1024 * 1024;
	
    /**
     * 绑定的数据传输IP地址
     */
    private String ip = "0.0.0.0";
    /**
     * 绑定的数据传输端口
     */
    private int port = 8066;
    
    private int max_allowed_packet = MAX_ALLOWED_PACKET;
    
    // a page size
 	private int bufferPoolPageSize = DEFAULT_BUFFER_POOL_PAGE_SIZE;
 	
 	//minimum allocation unit
 	private short bufferPoolChunkSize = DEFAULT_BUFFER_CHUNK_SIZE;
 	
    // buffer pool page number 
 	private short bufferPoolPageNumber = DEFAULT_BUFFER_POOL_PAGE_NUMBER;
 	
    /**
     * 是否使用动态配置的开关
     */
    private boolean annotationEnable;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

	public boolean isAnnotationEnable() {
		return annotationEnable;
	}

	public void setAnnotationEnable(boolean annotationEnable) {
		this.annotationEnable = annotationEnable;
	}
	
    @Override
    public String toString() {
        return "ProxyBean{" + "ip='" + ip + '\'' + ", port=" + port + ",annotationEnable="+annotationEnable+"}";
    }

	public int getBufferPoolPageSize() {
		return bufferPoolPageSize;
	}

	public void setBufferPoolPageSize(int bufferPoolPageSize) {
		this.bufferPoolPageSize = bufferPoolPageSize;
	}

	public short getBufferPoolChunkSize() {
		return bufferPoolChunkSize;
	}

	public void setBufferPoolChunkSize(short bufferPoolChunkSize) {
		if (bufferPoolChunkSize < 86){
			///cjw  2018.4.6 fix the HandshakePacket write proxybuffer which is low than 86 lead to error
			logger.warn("bufferPoolChunkSize should be greater than 86,and will be updated to 128;");
			bufferPoolChunkSize = 128;
		}
		this.bufferPoolChunkSize = bufferPoolChunkSize;
	}

	public short getBufferPoolPageNumber() {
		return bufferPoolPageNumber;
	}

	public void setBufferPoolPageNumber(short bufferPoolPageNumber) {
		this.bufferPoolPageNumber = bufferPoolPageNumber;
	}

	public int getMax_allowed_packet() {
		return max_allowed_packet;
	}

	public void setMax_allowed_packet(int max_allowed_packet) {
		this.max_allowed_packet = max_allowed_packet;
	}
}
