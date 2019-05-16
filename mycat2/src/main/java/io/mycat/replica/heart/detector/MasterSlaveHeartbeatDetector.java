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
import io.mycat.replica.heart.callback.SingleHeartBeatAsyncTaskCallBack;


public class MasterSlaveHeartbeatDetector extends AbstractHeartBeatDetector implements HeartbeatDetector {
    private final int slaveThreshold = 1000;
    public MasterSlaveHeartbeatDetector(ReplicaConfig replicaConfig, MySQLDataSourceEx dataSource, HeartbeatManager heartbeatManager) {
        super(replicaConfig, dataSource , heartbeatManager);
    }
    @Override
    public HeartBeatAsyncTaskCallBack getAsyncTaskCallback() {
        return new SingleHeartBeatAsyncTaskCallBack(this);
    }
//    @Override
//    public AsynTaskCallBack<MySQLSession> getAsyncTaskCallback(AtomicBoolean preIsQuit) {
//        AtomicBoolean isQuit = preIsQuit;
//        return (mysql, sender, success, result, errorMessage) -> {
//            if(isQuit.get() == false) {
////                String sql = replicaConfig.getRepType().getHearbeatSQL();
//                String sql = null;
//                new MulResultReadTask().request(mysql, sql, (session, sender1, success1, result1, errorMessage1) -> {
//                    if(isQuit.get() == false) {
//                        this.lastReceivedQryTime = System.currentTimeMillis();
//                        DatasourceStatus datasourceStatus = new DatasourceStatus();
//                        if (success1) {
//                            datasourceStatus = processHearbeatResult((List<Map<String,String>>)result1);
//                            List<Map<String,String>> resultList = (List<Map<String,String>>)result1;
//                            heartbeat.setStatus(datasourceStatus, DatasourceStatus.OK_STATUS);
//                        } else {
//                            heartbeat.setStatus(datasourceStatus, DatasourceStatus.ERROR_STATUS);
//                        }
//                    }
//                });
//                //单行结果集合获取
//                DatasourceStatus dataSourceStatus = new DatasourceStatus();
//                this.heartbeat.setStatus(dataSourceStatus, DatasourceStatus.OK_STATUS);
//            }
//            //设置成功的状态
//        };
//    }

//    protected DatasourceStatus processHearbeatResult(List<Map<String, String>> resultList) {
//        DatasourceStatus datasourceStatus = new DatasourceStatus();
//         Map<String,String>  resultResult= resultList.get(0);
//        String Slave_IO_Running  = resultResult != null ? resultResult.get("Slave_IO_Running") : null;
//        String Slave_SQL_Running = resultResult != null ? resultResult.get("Slave_SQL_Running") : null;
//        if (Slave_IO_Running != null
//                && Slave_IO_Running.equals(Slave_SQL_Running)
//                && Slave_SQL_Running.equals("Yes")) {
//            datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_NORMAL);
//            String Seconds_Behind_Master = resultResult.get( "Seconds_Behind_Master");
//            if (null != Seconds_Behind_Master && !"".equals(Seconds_Behind_Master)) {
//
//                int Behind_Master = Integer.parseInt(Seconds_Behind_Master);
//                if ( Behind_Master >  slaveThreshold ) {
//                    datasourceStatus.setSlaveBehindMaster(true);
//                    System.out.println("found MySQL master/slave Replication delay !!! "+
//                            " binlog sync time delay: " + Behind_Master + "s" );
//                } else {
//                    datasourceStatus.setSlaveBehindMaster(false);
//                }
//            }
//
//        } else if( dataSource.isSlave() ) {
//            String Last_IO_Error = resultResult != null ? resultResult.get("Last_IO_Error") : null;
//            System.out.println("found MySQL master/slave Replication err !!! "
//                    +   Last_IO_Error);
//            datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_ERROR);
//        }
//        datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
//        return datasourceStatus;
//    }

//    public HeartbeatInfReceiver<DatasourceStatus> getHeartbeatInfReceiver() {
//        return datasourceStatus -> {
//            return DatasourceStatus.OK_STATUS == datasourceStatus.getStatus()
//                    && datasourceStatus.getDbSynStatus() == DatasourceStatus.OK_STATUS
//                    && !datasourceStatus.isSlaveBehindMaster();
//        };
//
//    }

}
