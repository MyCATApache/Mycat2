package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.conf.ReplicaBean;
import io.mycat.mycat2.beans.heartbeat.DBHeartbeat;
import io.mycat.mycat2.beans.heartbeat.MySQLDetector;
import io.mycat.mycat2.beans.heartbeat.MySQLHeartbeat;
import io.mycat.mysql.packet.CommandPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class BackendHeartbeatTask extends BackendIOTaskWithResultSet<MySQLSession> {
    private static final String Slave_IO_Running_str = "Slave_IO_Running";

    private int fieldCount;
    private List<String> fetchColPos;
    Map<String, byte[]> result = new HashMap<>();
    private MySQLDetector detector;
    private MySQLRepBean repBean;
    private MySQLMetaBean metaBean;
    private MySQLSession optSession;
    private static final String wsrep_connected_str = "wsrep_connected";
    private static final String Slave_SQL_Running_str = "Slave_SQL_Running";
    private static final String Seconds_Behind_Master_str = "Seconds_Behind_Master";
    private static final byte[] YES = "Yes".getBytes();
    private static final byte[] ON = "ON".getBytes();
    private static final byte[] Primary = "Primary".getBytes();
    private static final String wsrep_cluster_status_str = "wsrep_cluster_status";
    private static final String wsrep_ready_str = "wsrep_ready";
    private static Logger logger = LoggerFactory.getLogger(BackendHeartbeatTask.class);


    public BackendHeartbeatTask(MySQLSession optSession, MySQLDetector detector) {
        this.detector = detector;
        this.metaBean = detector.getHeartbeat().getSource();
        this.repBean = metaBean.getRepBean();
        this.optSession = optSession;
    }

    public void doHeartbeat() {
        optSession.proxyBuffer.reset();
        CommandPacket packet = new CommandPacket();
        packet.packetId = 0;
        packet.command = MySQLCommand.COM_QUERY;
        packet.arg = repBean.getReplicaBean().getRepType().getHearbeatSQL();
        packet.write(optSession.proxyBuffer);
        packet = null;
        optSession.proxyBuffer.flip();
        optSession.proxyBuffer.readIndex = optSession.proxyBuffer.writeIndex;
        try {
            optSession.writeToChannel();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(" The backend heartbeat task write to mysql is error . {}", e.getMessage());
            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
            detector.getHeartbeat().setResult(DBHeartbeat.ERROR_STATUS, detector, null);
        }
    }

    @Override
    void onRsColCount(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
        //读取有多少列
        fieldCount = (int) proxyBuffer.getLenencInt(curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize);
        fetchColPos = new ArrayList<>(fieldCount);
    }

    @Override
    void onRsColDef(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
//        byte[] bytes = proxyBuffer.getBytes(curMQLPackgInf.startPos+MySQLPacket.packetHeaderSize+1,
//        									curMQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1);
        int tmpReadIndex = proxyBuffer.readIndex;
        proxyBuffer.readIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;
        proxyBuffer.readLenencString();  //catalog
        proxyBuffer.readLenencString();  //mycatSchema
        proxyBuffer.readLenencString();  //table
        proxyBuffer.readLenencString();  //orgTable
        String name = proxyBuffer.readLenencString();  //name

        fetchColPos.add(name);
        proxyBuffer.readIndex = tmpReadIndex;
    }

    @Override
    void onRsRow(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;

        if (ReplicaBean.RepTypeEnum.GARELA_CLUSTER == repBean.getReplicaBean().getRepType()) {
            int lenc = (int) proxyBuffer.getLenencInt(rowDataIndex);
            rowDataIndex += ProxyBuffer.getLenencLength(lenc);
            String key = proxyBuffer.getFixString(rowDataIndex, lenc);
            rowDataIndex += lenc;
            byte[] value = proxyBuffer.getLenencBytes(rowDataIndex);
            result.put(key, value);
        } else {
            //读取每行的各列数据
            for (int i = 0; i < fieldCount; i++) {
                int lenc = (int) proxyBuffer.getLenencInt(rowDataIndex);
                rowDataIndex += ProxyBuffer.getLenencLength(lenc);
                result.put(fetchColPos.get(i), proxyBuffer.getBytes(rowDataIndex, lenc));
                rowDataIndex += lenc;
            }
        }
    }

    @Override
    void onRsFinish(MySQLSession session, boolean success, String msg) {
        if (success) {
            //归还连接
            MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
            session.proxyBuffer.reset();

            optSession.setIdle(true);
            reactor.addMySQLSession(metaBean, session);

            switch (repBean.getReplicaBean().getRepType()) {
                case MASTER_SLAVE:
                    masterSlaveHeartbeat();
                    break;
                case GARELA_CLUSTER:
                    clusterHeartbeat();
                    break;
                case SINGLE_NODE:
                    detector.getHeartbeat().setResult(MySQLHeartbeat.OK_STATUS, detector, null);
                    break;
                default:
                    break;
            }
            detector.setLasstReveivedQryTime(System.currentTimeMillis());
        } else {
            if (ResultSetState.RS_STATUS_READ_ERROR == curRSState ||
                    ResultSetState.RS_STATUS_WRITE_ERROR == curRSState) {
                detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
                detector.getHeartbeat().setResult(DBHeartbeat.ERROR_STATUS, detector, null);
            }
            session.close(false, msg);
        }
    }

    private void masterSlaveHeartbeat() {

        if ((result == null || result.isEmpty())) {
            if (metaBean.isSlaveNode()) {
                logger.warn(" MySQL master/slave Replication has not found! ");
                logger.warn(" the current replica is in MASTER_SLAVE or GROUP_REPLICATION mode ?.{}:{}", metaBean.getDsMetaBean().getIp(), metaBean.getDsMetaBean().getPort());
                detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
                detector.getHeartbeat().setResult(DBHeartbeat.ERROR_CONF, detector, null);
            } else {
                detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
                detector.getHeartbeat().setResult(DBHeartbeat.OK_STATUS, detector, null);
            }
            return;
        }

        //配置双主时, 主一挂掉,主二 承接过来时,复制状态有可能还没有调整.这里暂时不检查切换过来的主的状态.
        if (!metaBean.isSlaveNode()) {
            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
            detector.getHeartbeat().setResult(DBHeartbeat.OK_STATUS, detector, null);
        }

        byte[] slave_io = result.get(Slave_IO_Running_str);
        if (slave_io != null
                && Arrays.equals(YES, slave_io)
                && Arrays.equals(YES, result.get(Slave_SQL_Running_str))) {
            String Seconds_Behind_Master = new String(result.get(Seconds_Behind_Master_str));

            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);

            if (null != Seconds_Behind_Master && !"".equals(Seconds_Behind_Master)) {
                int Behind_Master = Integer.parseInt(Seconds_Behind_Master);
                if (metaBean.getSlaveThreshold() >= 0 && Behind_Master > metaBean.getSlaveThreshold()) {
                    logger.warn("found MySQL master/slave Replication delay !!! "
                            + metaBean + ", binlog sync time delay: " + Behind_Master + "s");
                }
                detector.getHeartbeat().setSlaveBehindMaster(Behind_Master);
            }
//				detector.getHeartbeat().getAsynRecorder().set(resultResult, switchType);
        } else if (metaBean.isSlaveNode()) {
            //String Last_IO_Error = resultResult != null ? resultResult.get("Last_IO_Error") : null;
            logger.warn("found MySQL master/slave Replication err !!! {}:{}", metaBean.getDsMetaBean().getIp(), metaBean.getDsMetaBean().getPort());
            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
            detector.getHeartbeat().setResult(DBHeartbeat.ERROR_STATUS, detector, null);
            return;
        }
//		heartbeat.getAsynRecorder().set(resultResult, switchType);
        detector.getHeartbeat().setResult(DBHeartbeat.OK_STATUS, detector, null);
    }

    /**
     * garela_cluster 心跳检测
     */
    private void clusterHeartbeat() {

        if (result == null || result.isEmpty()) {
            logger.warn(" MySQL master/slave Replication has not found! ");
            logger.warn(" the current replica is in MASTER_SLAVE or GROUP_REPLICATION mode ?" + metaBean);
            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
            detector.getHeartbeat().setResult(DBHeartbeat.ERROR_CONF, detector, null);
            return;
        }

        //String Variable_name = resultResult != null ? resultResult.get("Variable_name") : null;
        byte[] wsrep_cluster_status = result.get(wsrep_cluster_status_str);// Primary
        byte[] wsrep_connected = result.get(wsrep_connected_str);// ON
        byte[] wsrep_ready = result.get(wsrep_ready_str);// ON

        if (wsrep_connected != null
                && Arrays.equals(ON, wsrep_connected)
                && Arrays.equals(ON, wsrep_ready)
                && Arrays.equals(Primary, wsrep_cluster_status)) {
            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
            detector.getHeartbeat().setResult(DBHeartbeat.OK_STATUS, detector, null);
        } else {
            logger.warn("found MySQL  cluster status err !!! "
                    + metaBean.getDsMetaBean().getIp() + ":" + metaBean.getDsMetaBean().getPort()
                    + " wsrep_cluster_status: " + new String(wsrep_cluster_status)
                    + " wsrep_connected: " + new String(wsrep_connected)
                    + " wsrep_ready: " + new String(wsrep_ready)
            );

            detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
            detector.getHeartbeat().setResult(DBHeartbeat.ERROR_STATUS, detector, null);
        }
//			detector.getHeartbeat().getAsynRecorder().set(resultResult, switchType);
    }
}
