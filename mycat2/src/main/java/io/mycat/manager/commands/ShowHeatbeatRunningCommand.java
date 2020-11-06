//package io.mycat.manager.commands;
//
//import io.mycat.MycatDataContext;
//import io.mycat.beans.mycat.ResultSetBuilder;
//import io.mycat.client.MycatRequest;
//import io.mycat.replica.ReplicaSelectorRuntime;
//import io.mycat.util.Response;
//
//import java.sql.JDBCType;
//import java.util.Arrays;
//
//public class ShowHeatbeatRunningCommand implements ManageCommand {
//    @Override
//    public String statement() {
//        return "show @@backend.heartbeat.running";
//    }
//
//    @Override
//    public String description() {
//        return statement();
//    }
//
//    @Override
//    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
//        try {
//            boolean heartbeat = ReplicaSelectorRuntime.INSTANCE.isHeartbeat();
//            ResultSetBuilder builder = ResultSetBuilder.create();
//            builder.addColumnInfo("running", JDBCType.BOOLEAN);
//            builder.addObjectRowPayload(Arrays.asList(heartbeat));
//            response.sendResultSet(()->builder.build());
//        } catch (Throwable e) {
//            response.sendError(e);
//        }
//    }
//}