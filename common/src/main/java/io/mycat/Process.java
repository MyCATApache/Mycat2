package io.mycat;

import io.vertx.sqlclient.SqlConnection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.*;
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
    private static final ThreadLocal<Process> PROCESS_THREAD_LOCAL = new ThreadLocal<>();
    private static final Set<Process> PROCESS_LIST = new LinkedHashSet<>();
    private static final AtomicInteger ID_INCR = new AtomicInteger();
    /**
     * 哪个后端在用哪个连接 (kill命令会 关闭进行中的链接)
     */
    private final LinkedList<SqlConnection> connections = new LinkedList<>();
    private final Map<Thread, Integer> connectionCounterMap = new HashMap<>();
    private final Timestamp createTimestamp = new Timestamp(System.currentTimeMillis());
    private final List<Thread> threadTraceList = new ArrayList<>();
    private String query;
    private Command command;
    private State state;
    private MycatDataContext context;

    public static Process getCurrentProcess() {
        Process process = PROCESS_THREAD_LOCAL.get();
        if (process == null) {
            log.error("mycat getCurrentProcess is null");
        }
        return process;
    }

    public static void setCurrentProcess(Process process) {
        if (process == null) {
            PROCESS_THREAD_LOCAL.remove();
        } else {
            PROCESS_THREAD_LOCAL.set(process);
        }
    }

    public static Set<Process> getProcessList() {
        return Collections.unmodifiableSet(PROCESS_LIST);
    }

    public static Process getProcess(int id) {
        for (Process process : PROCESS_LIST) {
            if (process.getId() == id) {
                return process;
            }
        }
        return null;
    }

    public static Process createProcess() {
        Process process = new Process();
        PROCESS_LIST.add(process);
        return process;
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
        return "";
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
        exit();
        context.close();
    }

    public String getDb() {
        return context.getDefaultSchema();
    }

    public void exit() {
        Set<SqlConnection> snapshot = new LinkedHashSet<>(connections);
        for (SqlConnection sqlConnection : snapshot) {
            sqlConnection.close();
        }
        PROCESS_LIST.remove(this);
        setCurrentProcess(null);
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

    public void exit(Thread thread) {
        synchronized (connectionCounterMap) {
            Integer count = connectionCounterMap.get(thread);
            if (count != null) {
                for (int i = 0; i < count; i++) {
                    connections.removeFirst();
                }
            }
        }
        setCurrentProcess(null);
    }

    @Override
    public String toString() {
        return "[" + getId() + "]" + query;
    }

    /**
     * show processlist 的Command字段
     */
    @Getter
    @AllArgsConstructor
    public enum Command {
        /**/
        SLEEP(0),
        QUIT(1),
        INIT_DB(2),
        QUERY(3);
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
