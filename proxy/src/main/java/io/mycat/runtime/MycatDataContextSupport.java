package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.TransactionSessionRunner;
import io.mycat.config.ServerConfig;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.ServerTransactionSessionRunner;
import io.mycat.thread.GThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public enum MycatDataContextSupport {

    INSTANCE;
    private Map<String, Function<MycatDataContext, TransactionSession>> transcationFactoryMap;
    private GThreadPool gThreadPool;

    public void init(ServerConfig.Worker worker, Map<String, Function<MycatDataContext, TransactionSession>> transcationFactoryMap) {
        Map<String, Function<MycatDataContext, TransactionSession>> map = new HashMap<>();
        final String proxy = "proxy";
        if (!transcationFactoryMap.containsKey("local")) {
            map.put("local", mycatDataContext -> new LocalTransactionSession(mycatDataContext));
        }
        if (!transcationFactoryMap.containsKey("proxy")) {
            map.put("proxy", mycatDataContext -> new ProxyTransactionSession(mycatDataContext));
        }
        map.putAll(transcationFactoryMap);
        this.transcationFactoryMap = map;
//        transcationFactoryMap.put("xa", sessionOpt -> new JTATransactionSession(new UserTransactionImp()));
//        transcationFactoryMap.put("proxy", sessionOpt -> new ProxyTransactionSession());

        this.gThreadPool = new GThreadPool(worker);
    }

    public TransactionSessionRunner createRunner(MycatSession session) {
        return new ServerTransactionSessionRunner(transcationFactoryMap, gThreadPool, session);
    }

    public MycatDataContext createDataContext(MycatSession session) {
        MycatDataContextImpl mycatDataContext = new MycatDataContextImpl(createRunner(session));
        mycatDataContext.switchTransaction("proxy");
        return mycatDataContext;
    }


}