/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
