package io.mycat;

import io.vertx.sqlclient.SqlConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Setter
@Getter
public class ProcessImpl implements Process {
    /**
     * 哪个后端在用哪个连接 (kill命令会 关闭进行中的链接)
     */
    private final Deque<SqlConnection> connections = new ConcurrentLinkedDeque<>();
    private final Map<Thread, Integer> connectionReferenceCountMap = new HashMap<>();
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

    public ProcessImpl() {
        getHoldThreads().add(Thread.currentThread());
    }

    @Override
    public void retain() {
        referenceCount.incrementAndGet();
        holdThreads.add(Thread.currentThread());
        Process.setCurrentProcess(this);
    }

    @Override
    public void release() {
        int ref = referenceCount.decrementAndGet();
        holdThreads.remove(Thread.currentThread());
        Process.setCurrentProcess(null);
        if (ref == 0) {
            exit();
        }
    }

    private void init(MycatDataContext context) {

    }

    @Override
    public long getId() {
        return context == null ? -1 : context.getSessionId();
    }

    @Override
    public String getUser() {
        return context.getUser().getUserName();
    }

    @Override
    public String getHost() {
        return context.getUser().getHost();
    }

    @Override
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

    @Override
    public void setContext(MycatDataContext context) {
        init(context);
        this.context = context;
    }

    @Override
    public void setCommand(int command) {
        this.command = Command.codeOf(command);
    }

    @Override
    public void kill() {
        closeConnection();
        Thread currentThread = Thread.currentThread();
        for (Thread holdThread : holdThreads) {
            if (holdThread == currentThread) {
                continue;
            }
            holdThread.interrupt();
        }
        exit();
        context.close();
    }

    @Override
    public String getDb() {
        return context.getDefaultSchema();
    }

    private void exit() {
        for (Thread holdThread : holdThreads) {
            PROCESS_MAP.remove(holdThread);
        }
        holdThreads.clear();
        Process.setCurrentProcess(null);
    }

    private void closeConnection() {
        Set<SqlConnection> snapshot = new LinkedHashSet<>(connections);
        for (SqlConnection sqlConnection : snapshot) {
            sqlConnection.close();
        }
        connectionReferenceCountMap.clear();
    }

    @Override
    public <T extends SqlConnection> T trace(T connection) {
        Thread thread = Thread.currentThread();
        synchronized (connectionReferenceCountMap) {
            Integer count = connectionReferenceCountMap.get(thread);
            connectionReferenceCountMap.put(thread, count == null ? 1 : count + 1);
            connections.addFirst(connection);
        }
        return connection;
    }

    @Override
    public String toString() {
        return "[" + getId() + "] " + query;
    }

}
