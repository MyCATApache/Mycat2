//package io.mycat.taskmanager;
//
//import io.mycat.TransactionSession;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public abstract class Task implements Runnable {
//    protected final TransactionSession session;
//    protected long memory;
//    protected long cpu;
//    protected Map<String, Integer> resources;
//    protected long expirationTime;
//    public Task(TransactionSession session,  Map<String, Integer> resources){
//        this(session,0,0,resources,0);
//    }
//    public Task(TransactionSession session, long memory, long cpu, Map<String, Integer> resources,long expirationTime) {
//        this.session = session;
//        this.memory = memory;
//        this.cpu = cpu;
//        this.resources = resources;
//        this.expirationTime = expirationTime;
//    }
//
//    public void onStart() {
//
//    }
//
//   abstract public void execute();
//
//
//    @Override
//    public void run() {
//        onStart();
//        session.bindContext();
//        try {
//            execute();
//        } finally {
//            session.unBindContext();
//        }
//    }
//
//    public TransactionSession getSession() {
//        return session;
//    }
//
//    public long getMemory() {
//        return memory;
//    }
//
//    public long getCpu() {
//        return cpu;
//    }
//
//    public List<String> getResources() {
//        return resources;
//    }
//
//    public long getExpirationTime() {
//        return expirationTime;
//    }
//}
