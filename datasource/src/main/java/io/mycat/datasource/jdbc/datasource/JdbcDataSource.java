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
package io.mycat.datasource.jdbc.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatServer;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.DatasourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class JdbcDataSource implements MycatDataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDataSource.class);
    private final DatasourceConfig datasourceConfig;
    final AtomicInteger counter = new AtomicInteger(0);
    final DataSource dataSource;
    volatile boolean valid = true;

    public JdbcDataSource(DatasourceConfig datasourceConfig, DataSource dataSource) {
        this.datasourceConfig = datasourceConfig;
        this.dataSource = dataSource;
    }

    public String getUrl() {
        return datasourceConfig.getUrl();
    }

    public String getUsername() {
        return datasourceConfig.getUser();
    }

    public String getPassword() {
        return datasourceConfig.getPassword();
    }

    public String getName() {
        return datasourceConfig.getName();
    }

    @Override
    public boolean isValid() {
        return false;
    }

    public boolean isMySQLType() {
        return "mysql".equalsIgnoreCase(datasourceConfig.getDbType());
    }


    public int getMaxCon() {
        return datasourceConfig.getMaxCon();
    }

    public String getDbType() {
        return datasourceConfig.getDbType();
    }

    public DatasourceConfig getConfig() {
        return datasourceConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcDataSource that = (JdbcDataSource) o;
        return datasourceConfig != null ? datasourceConfig.equals(that.datasourceConfig)
                : that.datasourceConfig == null;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (datasourceConfig != null ? datasourceConfig.hashCode() : 0);
        return result;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public int getUsedCount() {
        return counter.get();
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public void close() {
        int count = this.counter.get();
        if (count > 0) {
            if (this.datasourceConfig.isRemoveAbandoned() && this.getDataSource() instanceof DruidDataSource) {
                DruidDataSource druidDataSource = (DruidDataSource) this.getDataSource();
                Object activeConnections = druidDataSource.getActiveConnectionStackTrace();
                int activeCount = druidDataSource.getActiveCount();
                if (activeCount != count) {
                    LOGGER.error("JdbcDataSource:{} close activeCount{},activeCount != count",getName(),activeCount);
                }
                LOGGER.debug("JdbcDataSource:{} close ,but activeCount has {}", getName(), activeCount);
                LOGGER.debug("JdbcDataSource:{} close count but has activeConnections {}", getName(), activeConnections);
            }
            MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);
            RowBaseIterator rowBaseIterator = mycatServer.showConnections();
            List<Map<String, Object>> resultSetMap = rowBaseIterator.getResultSetMap();
            LOGGER.error(resultSetMap.toString());
            LOGGER.error(resultSetMap.toString());
        }
        Optional.ofNullable(this.getDataSource()).ifPresent(i -> {
            try {
                Class<? extends DataSource> aClass = i.getClass();
                Method[] methods = aClass.getMethods();
                ArrayList<Method> methodList = new ArrayList<>();
                for (Method method : methods) {
                    if ("close".equals(method.getName())) {
                        if (Void.TYPE.equals(method.getReturnType())) {
                            methodList.add(method);
                        }
                    }
                }
                methodList.sort(Comparator.comparingInt(Method::getParameterCount));
                if (!methodList.isEmpty()) {
                    methodList.get(0).invoke(i);
                    LOGGER.info("JdbcDataSource:{} closed", getName());
                }
            } catch (Throwable e) {
                LOGGER.error("试图关闭数据源失败:{} ,{}", getName(), e);
            }
        });
    }
}
