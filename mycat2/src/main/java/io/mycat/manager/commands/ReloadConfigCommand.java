//package io.mycat.manager.commands;
//
//import io.mycat.*;
//import io.mycat.booster.BoosterRuntime;
//import io.mycat.client.InterceptorRuntime;
//import io.mycat.client.MycatRequest;
//import io.mycat.metadata.MetadataManager;
//import io.mycat.proxy.reactor.MycatReactorThread;
//import io.mycat.proxy.session.MycatSession;
//import io.mycat.replica.ReplicaSelectorRuntime;
//import io.mycat.runtime.ProxySwitch;
//import io.mycat.util.Response;
//
//import java.util.concurrent.TimeUnit;
//
//
///**
// * 暂不可用
// */
//public class ReloadConfigCommand implements ManageCommand {
//    @Override
//    public String statement() {
//        return "reload @@config by file";
//    }
//
//    @Override
//    public String description() {
//        return statement();
//    }
//
//    @Override
//    public void handle(MycatRequest request, MycatDataContext context, Response response) {
//        try {
//            if (ProxySwitch.INSTANCE.stopRunning()) {
//                try {
//                    boolean inTransaction = waitNoTranscation();
//                    if (!inTransaction) {
//                        switchConfig();
//                        response.sendOk();
//                        return;
//                    } else {
//                        response.sendError(new MycatException("sessions are still in Transaction"));
//                        return;
//                    }
//                } finally {
//                    ProxySwitch.INSTANCE.continueRunning();
//                }
//            } else {
//                response.sendError(new MycatException("proxy has stopped"));
//                return;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            response.sendError(e);
//        }
//    }
//
//    private boolean waitNoTranscation() {
//        boolean inTransaction = false;
//        long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
//        while (true) {
//            for (MycatReactorThread mycatReactorThread : MycatCore.INSTANCE.getReactorManager().getList()) {
//                for (MycatSession allSession : mycatReactorThread.getFrontManager().getAllSessions()) {
//                    inTransaction = inTransaction || allSession.isInTransaction();
//                    if (inTransaction) {
//                        break;
//                    }
//                }
//            }
//            if (!inTransaction) {
//                break;//都没有事务则结束
//            }
//            if (endTime - System.currentTimeMillis() < 0) {
//                break;//超时结束
//            } else {
//                continue;//继续循环
//            }
//        }
//        return inTransaction;
//    }
//
//    private void switchConfig() throws Exception {
//        MycatConfig oldConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();
//        RootHelper.INSTANCE.getConfigProvider().fetchConfig();
//        MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();
//
//        PlugRuntime.INSTANCE.load(mycatConfig);
//        MycatWorkerProcessor.INSTANCE.init(mycatConfig.getServer().getWorkerPool(), mycatConfig.getServer().getTimeWorkerPool());
//        ReplicaSelectorRuntime.INSTANCE.load(mycatConfig);
//        JdbcRuntime.INSTANCE.load(mycatConfig);
//        BoosterRuntime.INSTANCE.load(mycatConfig);
//        InterceptorRuntime.INSTANCE.load(mycatConfig);
//        MetadataManager.INSTANCE.load(mycatConfig);
//        MycatCore.INSTANCE.flash(mycatConfig);
//    }
//}