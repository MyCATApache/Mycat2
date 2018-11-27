package io.mycat.mycat2.beans.conf;

import io.mycat.mycat2.beans.GlobalBean;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class DatasourceMetaBean {
    private String hostName;
    private String ip;
    private int port;
    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;
    private int maxRetryCount = GlobalBean.MAX_RETRY_COUNT;

    
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatasourceMetaBean other = (DatasourceMetaBean) obj;
		if (hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!hostName.equals(other.hostName))
			return false;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

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

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
    }

    public int getMinCon() {
        return minCon;
    }

    public void setMinCon(int minCon) {
        this.minCon = minCon;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    @Override
    public String toString() {
        return "DatasourceMetaBean{" + "hostName='" + hostName + '\'' + ", ip='" + ip + '\'' + ", port=" + port + ", user='" + user + '\''
                + ", password='" + password + '\'' + ", maxCon=" + maxCon + ", minCon=" + minCon + ", maxRetryCount=" + maxRetryCount + '}';
    }
}
