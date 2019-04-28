package io.mycat.beans;


import java.util.Objects;

public class DatasourceMeta {
    public DatasourceMeta(String hostName, String ip, int port, String user, String password) {
        this.datasourceName = hostName;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    private final String datasourceName;
    private final String ip;
    private final int port;
    private final String user;
    private final String password;
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasourceMeta that = (DatasourceMeta) o;
        return port == that.port &&
                Objects.equals(datasourceName, that.datasourceName) &&
                Objects.equals(ip, that.ip) &&
                Objects.equals(user, that.user) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasourceName, ip, port, user, password);
    }

    public String getDatasourceName() {
        return datasourceName;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
