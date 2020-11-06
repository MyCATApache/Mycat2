//package io.mycat.runtime;
//
//import io.mycat.MycatDataContext;
//import io.mycat.MycatWorkerProcessor;
//import io.mycat.TransactionSession;
//import io.mycat.beans.mycat.TransactionType;
//import io.mycat.config.ThreadPoolExecutorConfig;
//import io.mycat.proxy.session.MycatContextThreadPool;
//import io.mycat.thread.SimpleMycatContextBindingThreadPool;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.function.Function;
//
//public enum MycatDataContextSupport {
//    INSTANCE;
//    private Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap;
//    private MycatContextThreadPool mycatContextThreadPool;
//
//    public void init(ThreadPoolExecutorConfig worker,
//                     Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap,
//                     MycatWorkerProcessor workerProcessor) {
//        Map<TransactionType, Function<MycatDataContext, TransactionSession>> map = new HashMap<>();
//        if (!transcationFactoryMap.containsKey(TransactionType.DEFAULT)) {
//            map.put(TransactionType.DEFAULT, mycatDataContext -> new ProxyTransactionSession(mycatDataContext));
//        }
//        map.putAll(transcationFactoryMap);
//        this.transcationFactoryMap = map;
//        this.mycatContextThreadPool =  new SimpleMycatContextBindingThreadPool(worker,workerProcessor.getMycatWorker());
//    }
//
//
//    public SimpleMycatContextBindingThreadPool getMycatContextThreadPool() {
//        return mycatContextThreadPool;
//    }
//}