package io.mycat.mycat2.ymlTest;

/**
 * Desc:
 *
 * @date: 09/09/2017
 * @author: gaozhiwen
 */
public class MySQL {
    private String hostName;
    private String ip;
    private int port;
    private String user;
    private String password;
    private int minCon;
    private int maxCon;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMinCon() {
        return minCon;
    }

    public void setMinCon(int minCon) {
        this.minCon = minCon;
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
    }

    @Override public String toString() {
        return "MySQL{" +
                "hostName='" + hostName + '\'' +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", minCon=" + minCon +
                ", maxCon=" + maxCon +
                '}';
    }
}
