package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.TransactionSessionRunner;
import io.mycat.config.ServerConfig;
import io.mycat.proxy.session.MycatSession;
import io.mycat.thread.GThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public enum MycatDataContextSupport {

    INSTANCE;
    private Map<String, Function<MycatDataContext, TransactionSession>> transcationFactoryMap;
    private GThreadPool gThreadPool;
    private String defaultTranscationType;

    public void init(ServerConfig.Worker worker, Map<String, Function<MycatDataContext, TransactionSession>> transcationFactoryMap, String defaultTranscationType) {
        this.defaultTranscationType = defaultTranscationType;
        Map<String, Function<MycatDataContext, TransactionSession>> map = new HashMap<>();
        final String proxy = "proxy";
        if (!transcationFactoryMap.containsKey(proxy)) {
            map.put("local", mycatDataContext -> new LocalTransactionSession(mycatDataContext));
        }
        map.putAll(transcationFactoryMap);
        this.transcationFactoryMap = map;
//        transcationFactoryMap.put("xa", sessionOpt -> new JTATransactionSession(new UserTransactionImp()));
//        transcationFactoryMap.put("proxy", sessionOpt -> new ProxyTransactionSession());

        this.gThreadPool = new GThreadPool(worker);
    }

    public TransactionSessionRunner createRunner(MycatSession session) {
        return new ServerTransactionSessionRunnerImpl(transcationFactoryMap, gThreadPool, session);
    }

    public MycatDataContext createDataContext(MycatSession session) {
        MycatDataContextImpl mycatDataContext = new MycatDataContextImpl(createRunner(session));
        mycatDataContext.switchTransaction(defaultTranscationType);
        return mycatDataContext;
    }


}