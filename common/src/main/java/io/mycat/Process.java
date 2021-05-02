package io.mycat;

import io.vertx.sqlclient.SqlConnection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 资源集合, 用于实现 show processlist, 和 kill 命令
 *
 * @author wangzihaogithub 2021年5月1日23:00:08
 */
@Slf4j
@Setter
@Getter
public class Process {
    private static final Map<Thread, Process> PROCESS_MAP = new ConcurrentHashMap<>();
    private static final AtomicInteger ID_INCR = new AtomicInteger();
    /**
     * 哪个后端在用哪个连接 (kill命令会 关闭进行中的链接)
     */
    private final Deque<SqlConnection> connections = new ConcurrentLinkedDeque<>();
    private final Map<Thread, Integer> connectionCounterMap = new HashMap<>();
    private final Timestamp createTimestamp = new Timestamp(System.currentTimeMillis());
    private final Set<Thread> holdThreads = Collections.newSetFromMap(new ConcurrentHashMap<>());
    /**
     * 引用计数法 (引用数量为0时候, 会触发{@link #exit()})
     */
    private final AtomicInteger referenceCount = new AtomicInteger();
    private String query;
    private Command command;
    private State state;
    private MycatDataContext context;

    public static Process getCurrentProcess() {
        Process process = PROCESS_MAP.get(Thread.currentThread());
        return process;
    }

    public static void setCurrentProcess(Process process) {
        Thread thread = Thread.currentThread();
        if (process == null) {
            PROCESS_MAP.remove(thread);
        } else {
            Process old = PROCESS_MAP.put(thread, process);
            if (old != null && old != process) {
                throw new IllegalStateException();
            }
        }
    }

    public static Map<Thread, Process> getProcessMap() {
        return PROCESS_MAP;
    }

    public static Process getProcess(int id) {
        for (Process process : PROCESS_MAP.values()) {
            if (process.getId() == id) {
                return process;
            }
        }
        return null;
    }

    public static Process createProcess() {
        Process process = new Process();
        process.getHoldThreads().add(Thread.currentThread());
        return process;
    }

    public void retain() {
        referenceCount.incrementAndGet();
        holdThreads.add(Thread.currentThread());
        setCurrentProcess(this);
    }

    public void release() {
        int ref = referenceCount.decrementAndGet();
        holdThreads.remove(Thread.currentThread());
        setCurrentProcess(null);
        if (ref == 0) {
            exit();
        }
    }

    public void init(MycatDataContext context) {

    }

    public long getId() {
        return context == null ? -1 : context.getSessionId();
    }

    public String getUser() {
        return context.getUser().getUserName();
    }

    public String getHost() {
        return context.getUser().getHost();
    }

    public String getInfo() {
        return query;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public void setContext(MycatDataContext context) {
        init(context);
        this.context = context;
    }

    public void setCommand(int command) {
        this.command = Command.codeOf(command);
    }

    public void kill() {
        closeConnection();
        for (Thread holdThread : holdThreads) {
            holdThread.interrupt();
        }
        exit();
        context.close();
    }

    public String getDb() {
        return context.getDefaultSchema();
    }

    private void exit() {
        for (Thread holdThread : holdThreads) {
            PROCESS_MAP.remove(holdThread);
        }
        holdThreads.clear();
        setCurrentProcess(null);
    }

    private void closeConnection() {
        Set<SqlConnection> snapshot = new LinkedHashSet<>(connections);
        for (SqlConnection sqlConnection : snapshot) {
            sqlConnection.close();
        }
        connectionCounterMap.clear();
    }

    public <T extends SqlConnection> T trace(T connection) {
        Thread thread = Thread.currentThread();
        synchronized (connectionCounterMap) {
            Integer count = connectionCounterMap.get(thread);
            connectionCounterMap.put(thread, count == null ? 1 : count + 1);
            connections.addFirst(connection);
        }
        return connection;
    }

    @Override
    public String toString() {
        return "[" + getId() + "] " + query;
    }

    /**
     * show processlist 的Command字段
     */
    @Getter
    @AllArgsConstructor
    public enum Command {
        /**
         * mysql_sleep
         */
        SLEEP(0),

        /**
         * mysql_close
         */
        QUIT(1),

        /**
         * mysql_select_db
         */
        INIT_DB(2),

        /**
         * mysql_real_query
         */
        QUERY(3),

        /**
         * mysql_list_fields
         */
        FIELD_LIST(4),

        /**
         * mysql_create_db (deprecated)
         */
        CREATE_DB(5),

        /**
         * mysql_drop_db (deprecated)
         */
        DROP_DB(6),

        /**
         * mysql_refresh
         */
        REFRESH(7),

        /**
         * mysql_shutdown
         */
        SHUTDOWN(8),

        /**
         * mysql_stat
         */
        STATISTICS(9),

        /**
         * mysql_list_processes
         */
        PROCESS_INFO(10),

        /**
         * none, this is an internal thread state
         */
        CONNECT(11),

        /**
         * mysql_kill
         */
        PROCESS_KILL(12),

        /**
         * mysql_dump_debug_info
         */
        DEBUG(13),

        /**
         * mysql_ping
         */
        PING(14),

        /**
         * none, this is an internal thread state
         */
        TIME(15),

        /**
         * none, this is an internal thread state
         */
        DELAYED_INSERT(16),

        /**
         * mysql_change_user
         */
        CHANGE_USER(17),

        /**
         * used by slave server mysqlbinlog
         */
        BINLOG_DUMP(18),

        /**
         * used by slave server to get master table
         */
        TABLE_DUMP(19),

        /**
         * used by slave to log connection to master
         */
        CONNECT_OUT(20),

        /**
         * used by slave to register to master
         */
        REGISTER_SLAVE(21),

        /**
         * mysql_stmt_prepare
         */
        STMT_PREPARE(22),

        /**
         * mysql_stmt_execute
         */
        STMT_EXECUTE(23),

        /**
         * mysql_stmt_send_long_data
         */
        STMT_SEND_LONG_DATA(24),

        /**
         * mysql_stmt_close
         */
        STMT_CLOSE(25),

        /**
         * mysql_stmt_reset
         */
        STMT_RESET(26),

        /**
         * mysql_set_server_option
         */
        SET_OPTION(27),

        /**
         * mysql_stmt_fetch
         */
        STMT_FETCH(28),

        /**
         * DAEMON
         */
        DAEMON(29),

        /**
         * RESET_CONNECTION
         */
        RESET_CONNECTION(31);

        private final int code;

        public static Command codeOf(int code) {
            for (Command value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            return null;
        }
    }

    /**
     * show processlist 的State字段
     */
    public enum State {
        /**/
        INIT
    }
}
