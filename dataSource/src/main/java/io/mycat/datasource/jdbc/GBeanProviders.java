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
package io.mycat.datasource.jdbc;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GBeanProviders {

  <T extends JdbcDataSource> T createJdbcDataSource(GRuntime runtime, int index,
      DatasourceConfig datasourceConfig,
      JdbcReplica mycatReplica);

  <T extends JdbcReplica> T createJdbcReplica(GRuntime runtime,
      Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList,
      DatasourceProvider provider);
}