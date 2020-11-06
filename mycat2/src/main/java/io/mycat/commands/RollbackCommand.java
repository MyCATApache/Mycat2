//package io.mycat.commands;
//
//import io.mycat.MycatDataContext;
//import io.mycat.client.MycatRequest;
//import io.mycat.util.Response;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
///**
// * @author Junwen Chen
// **/
//public enum RollbackCommand implements MycatCommand {
//    INSTANCE;
//   private static final Logger LOGGER = LoggerFactory.getLogger(RollbackCommand.class);
//
//    @Override
//    public boolean run(MycatRequest request, MycatDataContext context, Response response) throws Exception {
//        response.rollback();
//        return true;
//    }
//
//    @Override
//    public boolean explain(MycatRequest request, MycatDataContext context, Response response) throws Exception {
//        response.sendExplain(RollbackCommand.class,"rollback");
//        return true;
//    }
//
//    @Override
//    public String getName() {
//        return "rollback";
//    }
//}