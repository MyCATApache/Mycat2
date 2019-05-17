package io.mycat.replica.heart;

import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heart.detector.GarelaHeartbeatDetector;
import io.mycat.replica.heart.detector.MasterSlaveHeartbeatDetector;
import io.mycat.replica.heart.detector.SingleNodeHeartbeatDetector;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : zhangwy
 * @version V1.0
 * @date Date : 2019年05月14日 22:21
 */
public class MysqlHeartBeatManager implements HeartbeatManager{
    Logger logger = LoggerFactory.getLogger(MysqlHeartBeatManager.class);

    private final HeartbeatDetector heartbeatDetector;

//    private final ReplicaConfig replicaConfig;
    private final MySQLDatasource dataSource;
    private DatasourceStatus heartBeatStatus;

    protected volatile boolean isChecking = false; //是否正在检查
    protected AtomicInteger errorCount = new AtomicInteger(0); //错误计数
    protected int maxRetry = 3;

    public MysqlHeartBeatManager(ReplicaConfig replicaConfig, MySQLDataSourceEx dataSource){
//        this.replicaConfig = replicaConfig;
        this.dataSource = dataSource;

        heartBeatStatus = new DatasourceStatus();

        if(ReplicaConfig.RepTypeEnum.SINGLE_NODE.equals(replicaConfig.getRepType())) {
            this.heartbeatDetector = new SingleNodeHeartbeatDetector(replicaConfig, dataSource, this);
        } else  if(ReplicaConfig.RepTypeEnum.MASTER_SLAVE.equals(replicaConfig.getRepType())){
            this.heartbeatDetector = new MasterSlaveHeartbeatDetector(replicaConfig, dataSource, this);
        } else if(ReplicaConfig.RepTypeEnum.GARELA_CLUSTER.equals(replicaConfig.getRepType())){
            this.heartbeatDetector = new GarelaHeartbeatDetector(replicaConfig, dataSource, this);
        } else {
            this.heartbeatDetector = null;
        }

    }

    /**
     * 1: 发送sql
     * 判断是否已经发送 如果还未发送 则发送sql  设置为发送中。。
     * 如果为发送中。。 判断是否已经超时, 超时则设置超时状态  设置为未发送
     * 如果发生错误, 则设置错误状态.  设置为未发送
     * 2. 检查结果集 --> 设置结果集状态 设置为未发送
     * 设置失败集状态 设置为未发送
     * 设置超时状态
     */
    public void heartBeat(){
        if(isChecking == false) {
            isChecking = true;
//            this.heartbeatDetector.updateLastSendQryTime();
            this.heartbeatDetector.heartBeat();
        } else if(this.heartbeatDetector.isHeartbeatTimeout()) {
            DatasourceStatus datasourceStatus = new DatasourceStatus();
            datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
            this.setStatus(datasourceStatus, DatasourceStatus.TIMEOUT_STATUS);
        }
    }
    public void setStatus(DatasourceStatus datasourceStatus ,int status) {
        //对应的status 状态进行设置
        switch (status) {
            case DatasourceStatus.OK_STATUS:
                setOk(datasourceStatus);
                break;
            case DatasourceStatus.ERROR_STATUS:
                setError(datasourceStatus);
                break;
            case DatasourceStatus.TIMEOUT_STATUS:
                setTimeout(datasourceStatus);
                break;
        }

        this.heartbeatDetector.updateLastReceivedQryTime();
        isChecking = false;
    }

    private void setTimeout(DatasourceStatus datasourceStatus) {
        errorCount.incrementAndGet();
        heartbeatDetector.quitDetector();
        if(errorCount.get()  == maxRetry) {
            datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
            sendDataSourceStatus(datasourceStatus);
            errorCount.set(0);
        }
    }

    private void setError(DatasourceStatus datasourceStatus) {
        errorCount.incrementAndGet();
        heartbeatDetector.quitDetector();
        if(errorCount.get()  == maxRetry) {
            datasourceStatus.setStatus(DatasourceStatus.ERROR_STATUS);
            sendDataSourceStatus(datasourceStatus);
            errorCount.set(0);
        }
    }

    private void setOk(DatasourceStatus datasourceStatus) {
        //对应的status 状态进行设置
        switch (this.heartBeatStatus.getStatus()) {
            case DatasourceStatus.INIT_STATUS:
            case DatasourceStatus.OK_STATUS:
                datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
                errorCount.set(0);
                break;
            case DatasourceStatus.ERROR_STATUS:
                datasourceStatus.setStatus(DatasourceStatus.INIT_STATUS);
                errorCount.set(0);
                break;
            case DatasourceStatus.TIMEOUT_STATUS:
                datasourceStatus.setStatus(DatasourceStatus.INIT_STATUS);
                errorCount.set(0);
                break;
            default:
                datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
        }
        sendDataSourceStatus(datasourceStatus);
    }

    //给所有的mycatThread发送dataSourceStatus
    public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus ) {
        //状态不同进行状态的同步
        if(!this.heartBeatStatus.equals(currentDatasourceStatus)) {
            //设置状态给 dataSource
            this.heartBeatStatus = currentDatasourceStatus;
            logger.error("{} heartStatus {}", dataSource.getName(), heartBeatStatus);
            if(dataSource.isMaster() && heartBeatStatus.isError()) {
                //replicat 进行选主
                ((MySQLReplicaEx) dataSource.getReplica()).switchDataSourceIfNeed();
            }
        }
    }

    public DatasourceStatus getHeartBeatStatus() {
        return heartBeatStatus;
    }
}
