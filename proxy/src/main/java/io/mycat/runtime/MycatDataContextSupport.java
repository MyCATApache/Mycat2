package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.TransactionSessionRunner;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.config.ServerConfig;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.ServerTransactionSessionRunner;
import io.mycat.thread.GThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public enum MycatDataContextSupport {
    INSTANCE;
    private Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap;
    private GThreadPool gThreadPool;

    public void init(ServerConfig.Worker worker, Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap) {
        Map<TransactionType, Function<MycatDataContext, TransactionSession>> map = new HashMap<>();
        if (!transcationFactoryMap.containsKey(TransactionType.DEFAULT)) {
            map.put(TransactionType.DEFAULT, mycatDataContext -> new ProxyTransactionSession(mycatDataContext));
        }
        map.putAll(transcationFactoryMap);
        this.transcationFactoryMap = map;
        this.gThreadPool = new GThreadPool(worker);
    }

    public TransactionSessionRunner createRunner(MycatSession session) {
        return new ServerTransactionSessionRunner(transcationFactoryMap, gThreadPool, session);
    }

    public MycatDataContext createDataContext(MycatSession session) {
        MycatDataContextImpl mycatDataContext = new MycatDataContextImpl(createRunner(session));
        mycatDataContext.switchTransaction(TransactionType.PROXY_TRANSACTION_TYPE);
        return mycatDataContext;
    }


}