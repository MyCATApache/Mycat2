package io.mycat;

import io.vertx.sqlclient.SqlConnection;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 资源集合, 用于实现 show processlist, 和 kill 命令
 *
 * @author wangzihaogithub 2021年5月1日23:00:08
 */
public interface Process {
    Map<Thread, Process> PROCESS_MAP = new ConcurrentHashMap<>();
    Process EMPTY = new Process() {
        @Override
        public void setContext(MycatDataContext context) {

        }

        @Override
        public void setCommand(int command) {

        }

        @Override
        public void setQuery(String query) {

        }

        @Override
        public void setState(State state) {

        }
    };

    public static Process getCurrentProcess() {
        Process process = PROCESS_MAP.get(Thread.currentThread());
        return process == null ? EMPTY : process;
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

    default void retain() {
    }

    default void release() {
    }

    default long getId() {
        return -1;
    }

    default String getUser() {
        return null;
    }

    default State getState() {
        return null;
    }

    void setState(State state);

    default String getHost() {
        return null;
    }

    default String getInfo() {
        return null;
    }

    default Timestamp getCreateTimestamp() {
        return null;
    }

    default Command getCommand() {
        return null;
    }

    void setCommand(int command);

    void setContext(MycatDataContext context);

    void setQuery(String query);

    default void kill() {
    }

    default String getDb() {
        return null;
    }

    default <T extends SqlConnection> T trace(T connection) {
        return connection;
    }

    /**
     * show processlist 的Command字段
     */
    @Getter
    @AllArgsConstructor
    enum Command {
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

        static Command codeOf(int code) {
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
    enum State {
        /**/
        INIT
    }
}