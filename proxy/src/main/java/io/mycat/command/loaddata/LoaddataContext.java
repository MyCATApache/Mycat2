package io.mycat.command.loaddata;

import io.mycat.beans.MySQLSessionMonopolizeType;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.handler.backend.RequestHandler;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;

public class LoaddataContext {

  MySQLPayloadWriter context;

  public void append(byte[] data) {
    if (context == null) {
      context = new MySQLPayloadWriter(8192);
    }
    context.writeBytes(data);
  }

  public void proxy(MycatSession mycat,String replicaName,String defaultDataBaseName) {
    int packetId = 2;
    MySQLPacketSplitter splitter = new PacketSplitterImpl();
    splitter.init(context.size());
    while (splitter.nextPacketInPacketSplitter()) {
      packetId++;
    }
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(2, context.toByteArray());

    context = null;
    splitter = null;

    int emptyPacketId = packetId;

    mycat.setHandleContentOfFilename(false);
    MySQLClientSession mySQLSession = mycat.getMySQLSession();
    mySQLSession.setMonopolizeType(MySQLSessionMonopolizeType.NONE);
    RequestHandler.INSTANCE.request(mycat.getMySQLSession(), bytes, new RequestCallback() {
      @Override
      public void onFinishedSend(MySQLClientSession session, Object sender, Object attr) {
        byte[] emptyPacket = MySQLPacketUtil.generateMySQLPacket(emptyPacketId, new byte[]{});
        //@todo
//        MySQLProxyNIOHandler
//            .INSTANCE.proxyBackend(mycat, emptyPacket, targetName,defaultDataBaseName, null, ResponseType.QUERY,
//            MySQLProxyNIOHandler.INSTANCE, DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK
//        );
      }

      @Override
      public void onFinishedSendException(Exception e, Object sender, Object attr) {
        mycat.setMySQLSession(null);
        mycat.setLastMessage(e.toString());
        mycat.writeErrorEndPacketBySyncInProcessError();
      }
    });
  }
}