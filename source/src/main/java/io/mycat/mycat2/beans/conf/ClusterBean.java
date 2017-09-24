package io.mycat.mycat2.beans.conf;

/**
 * Desc: 集群配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class ClusterBean {
    private boolean enable = false;
    private String ip = "0.0.0.0";
    private int port = 9066;
    private String myNodeId;
    private String allNodes;
    /**
     * 用于集群中发送prepare报文等待确认的时间
     */
    private int prepareDelaySeconds = 30;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

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

    public String getMyNodeId() {
        return myNodeId;
    }

    public void setMyNodeId(String myNodeId) {
        this.myNodeId = myNodeId;
    }

    public String getAllNodes() {
        return allNodes;
    }

    public void setAllNodes(String allNodes) {
        this.allNodes = allNodes;
    }

    public int getPrepareDelaySeconds() {
        return prepareDelaySeconds;
    }

    public void setPrepareDelaySeconds(int prepareDelaySeconds) {
        this.prepareDelaySeconds = prepareDelaySeconds;
    }

    @Override
    public String toString() {
        return "ClusterBean{" + "enable=" + enable + ", ip='" + ip + '\'' + ", port=" + port + ", myNodeId='" + myNodeId + '\'' + ", allNodes='"
                + allNodes + '\'' + ", prepareDelaySeconds=" + prepareDelaySeconds + '}';
    }
}
