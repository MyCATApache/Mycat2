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
package io.mycat.replica.heart.detector;

import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.heart.AbstractHeartBeatDetector;
import io.mycat.replica.heart.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heart.HeartbeatDetector;
import io.mycat.replica.heart.HeartbeatManager;
import io.mycat.replica.heart.callback.MasterSlaveBeatAsyncTaskCallBack;
import io.mycat.replica.heart.callback.SingleHeartBeatAsyncTaskCallBack;


public class MasterSlaveHeartbeatDetector extends AbstractHeartBeatDetector implements HeartbeatDetector {
    private final int slaveThreshold = 1000;
    public MasterSlaveHeartbeatDetector(ReplicaConfig replicaConfig, MySQLDataSourceEx dataSource, HeartbeatManager heartbeatManager) {
        super(replicaConfig, dataSource , heartbeatManager);
    }
    @Override
    public HeartBeatAsyncTaskCallBack getAsyncTaskCallback() {
        return new MasterSlaveBeatAsyncTaskCallBack(this);
    }
}
