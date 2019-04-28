package io.mycat.proxy;

import io.mycat.beans.DataNode;
import io.mycat.beans.MySQLSchemaManager;
import io.mycat.config.MycatConfig;
import io.mycat.proxy.buffer.BufferPoolImpl;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MycatRuntime {
    public static final MycatRuntime INSTANCE = new MycatRuntime();
    private static final Logger logger = LoggerFactory.getLogger(MycatRuntime.class);
    private final AtomicInteger sessionIdCounter = new AtomicInteger(0);
    private final MycatConfig mycatConfig = new MycatConfig();
    private final MycatScheduler mycatScheduler = new MycatScheduler();

    public MySQLSchemaManager getMySQLSchemaManager() {
        return mySQLSchemaManager;
    }

    private final MySQLSchemaManager mySQLSchemaManager = new MySQLSchemaManager();
    public MycatScheduler getMycatScheduler() {
        return mycatScheduler;
    }

    private NIOAcceptor acceptor;
    private MycatReactorThread[] reactorThreads;

    public void initReactor() throws IOException {
        int cpus = Runtime.getRuntime().availableProcessors();
         //MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[Math.max(1,cpus-2)];
        MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[1];
        this.setMycatReactorThreads(mycatReactorThreads);
        for (int i = 0; i < mycatReactorThreads.length; i++) {
            mycatReactorThreads[i] = new MycatReactorThread(new BufferPoolImpl(), new MycatSessionManager());
            mycatReactorThreads[i].start();
        }
    }

    public void initHeartbeat() {
        this.getMycatScheduler().scheduleAtFixedRate(() -> {
            for (Replica replica : this.mycatConfig.getReplicaMap().values()) {
                replica.doHeartbeat();
            }
        }, Integer.MAX_VALUE, TimeUnit.DAYS);
    }

    public void initAcceptor() throws IOException {
        NIOAcceptor acceptor = new NIOAcceptor(new BufferPoolImpl());
        this.setAcceptor(acceptor);
        acceptor.start();
        acceptor.startServerChannel(mycatConfig.getIP(), mycatConfig.getPort());
    }

    public NIOAcceptor getAcceptor() {
        return acceptor;
    }

    public void setAcceptor(NIOAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    public void setMycatReactorThreads(MycatReactorThread[] reactorThreads) {
        this.reactorThreads = reactorThreads;
    }

    public int genSessionId() {
        return sessionIdCounter.incrementAndGet();
    }

    public MycatReactorThread[] getMycatReactorThreads() {
        return reactorThreads;
    }

    public MycatConfig getMycatConfig() {
        return mycatConfig;
    }

    public DataNode getDataNodeByName(String name) {
        return mycatConfig.getDataNodeByName(name);
    }
}
