package io.mycat.mycat2.beans.conf;

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
}
