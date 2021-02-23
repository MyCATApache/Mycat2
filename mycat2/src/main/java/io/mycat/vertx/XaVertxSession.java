//package io.mycat.vertx;
//
//import cn.mycat.vertx.xa.XaSqlConnection;
//import io.mycat.MycatDataContext;
//import io.mycat.proxy.session.ProcessState;
//import io.mycat.runtime.MycatDataContextImpl;
//import io.vertx.core.net.NetSocket;
//
//import java.nio.charset.Charset;
//
//public  abstract class XaVertxSession extends VertxSessionImpl {
//
//    public XaVertxSession(MycatDataContextImpl mycatDataContext, NetSocket socket,XaSqlConnection) {
//        super(mycatDataContext, socket);
//    }
//
//    @Override
//    public XaSqlConnection xaConnection() {
//        return super.xaConnection();
//    }
//}
