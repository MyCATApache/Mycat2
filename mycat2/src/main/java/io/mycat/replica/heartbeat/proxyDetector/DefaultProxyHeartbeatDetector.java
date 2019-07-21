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
package io.mycat.replica.heartbeat.proxyDetector;

import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.mysqlapi.collector.CommonSQLCallback;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import io.mycat.replica.heartbeat.HeartbeatManager;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.function.Function;


public class DefaultProxyHeartbeatDetector extends AbstractProxyHeartBeatDetector implements HeartbeatDetector<MySQLDatasource,ProxyHeartBeatAsyncTaskCallBack> {

    private final Function<HeartbeatDetector, CommonSQLCallback> commonSQLCallbacbProvider;

    public DefaultProxyHeartbeatDetector(ProxyRuntime runtime,
        ReplicaConfig replicaConfig, MySQLDataSourceEx dataSource,
        HeartbeatManager heartbeatManager,
        Function<HeartbeatDetector,CommonSQLCallback> commonSQLCallbacbProvider) {
        super(runtime, replicaConfig, dataSource, heartbeatManager);
        this.commonSQLCallbacbProvider = commonSQLCallbacbProvider;
    }

    @Override
    public ProxyHeartBeatAsyncTaskCallBack getCallback() {
        return new ProxyHeartBeatAsyncTaskCallBack(this,commonSQLCallbacbProvider.apply(this));
    }

}
