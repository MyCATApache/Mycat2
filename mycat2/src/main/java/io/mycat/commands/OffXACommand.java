//package io.mycat.commands;
//
//import io.mycat.MycatDataContext;
//import io.mycat.beans.mycat.TransactionType;
//import io.mycat.client.MycatRequest;
//import io.mycat.util.Response;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
///**
// * @author Junwen Chen
// **/
//public enum OffXACommand implements MycatCommand{
//    INSTANCE;
//    final static Logger LOGGER = LoggerFactory.getLogger(OffXACommand.class);
//    @Override
//    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
//        if (context.isInTransaction()) throw new IllegalArgumentException();
//        context.switchTransaction(TransactionType.PROXY_TRANSACTION_TYPE);
//        LOGGER.debug("session id:{} action:{}", request.getSessionId(), "set xa = 0 exe success");
//        response.sendOk();
//        return true;
//    }
//
//    @Override
//    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
//        response.sendExplain(OffXACommand.class,TransactionType.PROXY_TRANSACTION_TYPE.getName());
//        return true;
//    }
//
//    @Override
//    public String getName() {
//        return "offXA";
//    }
//}