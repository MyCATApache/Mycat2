/**
 * Copyright (C) <2019>  <chen junwen>
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

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.DatasourceRootConfig;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class JdbcDataSource implements MycatDataSource {
    private final DatasourceRootConfig.DatasourceConfig datasourceConfig;
    final AtomicInteger counter = new AtomicInteger(0);
    final DataSource dataSource;

    public JdbcDataSource(DatasourceRootConfig.DatasourceConfig datasourceConfig, DataSource dataSource) {
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

    public boolean isMySQLType() {
        return datasourceConfig.isMySQLType();
    }


    public int getMaxCon() {
        return datasourceConfig.getMaxCon();
    }

    public String getDbType() {
        return datasourceConfig.getDbType();
    }

    public DatasourceRootConfig.DatasourceConfig getConfig() {
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
}
