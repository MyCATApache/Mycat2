/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans;

import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.DatasourceRootConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MySQL Seesion元信息 对外支持线程修改的属性是alive,其他属性只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLDatasource implements MycatDataSource {

    final String ip;
    final int port;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MySQLDatasource that = (MySQLDatasource) o;

        return datasourceConfig != null ? datasourceConfig.equals(that.datasourceConfig) : that.datasourceConfig == null;
    }

    @Override
    public int hashCode() {
        return datasourceConfig != null ? datasourceConfig.hashCode() : 0;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDatasource.class);
    protected final DatasourceConfig datasourceConfig;
    protected final AtomicInteger connectionCounter = new AtomicInteger(0);
//    protected final AtomicInteger usedCounter = new AtomicInteger(0);

    public MySQLDatasource(DatasourceConfig datasourceConfig) {
        this.datasourceConfig = datasourceConfig;
        ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(datasourceConfig.getUrl());
        HostInfo hostInfo = connectionUrlParser.getHosts().get(0);
        this.ip = hostInfo.getHost();
        this.port = hostInfo.getPort();
    }

    public int getSessionLimitCount() {
        return datasourceConfig.getMaxCon();
    }

    public int getSessionMinCount() {
        return datasourceConfig.getMinCon();
    }

    @Override
    public String getName() {
        return this.datasourceConfig.getName();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return this.datasourceConfig.getUser();
    }

    public String getPassword() {
        return this.datasourceConfig.getPassword();
    }

    public int decrementSessionCounter() {
        return connectionCounter.updateAndGet(operand -> {
            if (operand > 0) {
                return --operand;
            } else {
                return 0;
            }
        });
    }

    public boolean tryIncrementSessionCounter() {
        return connectionCounter.updateAndGet(operand -> {
            if (!(operand < this.datasourceConfig.getMaxCon())) {
                return operand;
            } else {
                return ++operand;
            }
        }) < this.datasourceConfig.getMaxCon();
    }

    public String getInitSqlForProxy() {
        List<String> initSqls = datasourceConfig.getInitSqls();
        if (initSqls.isEmpty()) {
            return null;
        } else {
            return String.join(";", initSqls);
        }
    }


    public int gerMaxRetry() {
        return this.datasourceConfig.getMaxRetryCount();
    }

    public long getMaxConnectTimeout() {
        return this.datasourceConfig.getMaxConnectTimeout();
    }

    public long getIdleTimeout() {
        return this.datasourceConfig.getIdleTimeout();
    }

    public int getConnectionCounter() {
        return connectionCounter.get();
    }

}
