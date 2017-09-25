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

    @Override
    public String toString() {
        return "ProxyBean{" + "ip='" + ip + '\'' + ", port=" + port + '}';
    }
}
