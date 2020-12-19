/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasourceprovider;

import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.seata.rm.datasource.xa.DataSourceProxyXA;

/**
 * @author Junwen Chen
 **/
public class SeataXADatasourceProvider extends SeataATDatasourceProvider {

  @Override
  public JdbcDataSource createDataSource(DatasourceConfig config) {
    JdbcDataSource dataSource = super.createDataSource(config);
    return new JdbcDataSource(config,new DataSourceProxyXA(dataSource.getDataSource()));
  }

}