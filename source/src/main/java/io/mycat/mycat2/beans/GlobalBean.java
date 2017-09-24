package io.mycat.mycat2.beans;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class GlobalBean {
    public static final String SINGLE_NODE_HEARTBEAT_SQL = "select 1";
    public static final String MASTER_SLAVE_HEARTBEAT_SQL = "show slave status";
    public static final String GARELA_CLUSTER_HEARTBEAT_SQL = "show status like 'wsrep%'";
    public static final String GROUP_REPLICATION_HEARTBEAT_SQL = "show slave status";

    public static final String[] MYSQL_SLAVE_STAUTS_COLMS = {
            "Seconds_Behind_Master",
            "Slave_IO_Running",
            "Slave_SQL_Running",
            "Slave_IO_State",
            "Master_Host",
            "Master_User",
            "Master_Port",
            "Connect_Retry",
            "Last_IO_Error"};

    public static final String[] MYSQL_CLUSTER_STAUTS_COLMS = {"Variable_name", "Value"};

    public static final int INIT_VERSION = 1;
    // 默认的重试次数
    public static final int MAX_RETRY_COUNT = 5;
}
