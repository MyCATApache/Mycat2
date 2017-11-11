package io.mycat.mycat2.beans.conf;

import io.mycat.mycat2.beans.GlobalBean;

/**
 * Desc: mycat代理配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class ProxyBean {
	
    /**
     * 绑定的数据传输IP地址
     */
    private String ip = "0.0.0.0";
    /**
     * 绑定的数据传输端口
     */
    private int port = 8066;
    
    private int max_allowed_packet = GlobalBean.MAX_ALLOWED_PACKET;
    
    // a page size
 	private int bufferPoolPageSize = GlobalBean.DEFAULT_BUFFER_POOL_PAGE_SIZE;
 	
 	//minimum allocation unit
 	private short bufferPoolChunkSize = GlobalBean.DEFAULT_BUFFER_CHUNK_SIZE;
 	
    // buffer pool page number 
 	private short bufferPoolPageNumber = GlobalBean.DEFAULT_BUFFER_POOL_PAGE_NUMBER;
 	
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
