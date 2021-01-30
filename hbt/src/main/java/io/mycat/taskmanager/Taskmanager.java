//package io.mycat.taskmanager;
//
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.Promise;
//
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.Executor;
//import java.util.concurrent.Executors;
//
//public class Taskmanager {
//
//    public Taskmanager() {
//    }
//
//    private Executor scheduleResourceExecuter;
//
//    public Future<Void> scheduleResources(Task task) {
//       return Future.future(event -> {
//            try{
//                scheduleResourceExecuter.execute(task);
//                event.tryComplete();
//            }catch (Throwable throwable){
//                event.fail(throwable);
//            }
//
//        });
//    }
//
//    public void onStart() {
//        scheduleResourceExecuter = Executors.newSingleThreadExecutor();
//    }
//}
