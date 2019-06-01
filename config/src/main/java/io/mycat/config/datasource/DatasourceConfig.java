/**
 * Copyright (C) <2019>  <chen junwen,gaozhiwen>
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

package io.mycat.config.datasource;

import io.mycat.config.GlobalConfig;
import java.util.Objects;

/**
 * dataSource
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class DatasourceConfig {

    private String name;
    private String ip;
    private int port;
    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;
    private int maxRetryCount = GlobalConfig.MAX_RETRY_COUNT;
    private String dbType;
    private String url;
    private int weight = 0;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatasourceConfig that = (DatasourceConfig) o;
        return port == that.port &&
                   maxCon == that.maxCon &&
                   minCon == that.minCon &&
                   maxRetryCount == that.maxRetryCount &&
                   Objects.equals(name, that.name) &&
                   Objects.equals(ip, that.ip) &&
                   Objects.equals(user, that.user) &&
                   Objects.equals(password, that.password) &&
                   Objects.equals(dbType, that.dbType) &&
                   Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects
                   .hash(name, ip, port, user, password, maxCon, minCon, maxRetryCount, dbType,
                       url);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
}
