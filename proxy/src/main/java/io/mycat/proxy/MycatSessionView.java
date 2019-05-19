package io.mycat.proxy;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.handler.LocalInFileRequestHandler.LocalInFileSession;
import io.mycat.proxy.handler.MySQLPacketExchanger.MySQLProxyNIOHandler;
import io.mycat.proxy.handler.PrepareStatementHandler.PrepareStatementSession;
import io.mycat.proxy.packet.MySQLPacketUtil;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.security.MycatUser;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author jamie12221
 * @date 2019-05-12 22:41 mycat session用户视图,屏蔽proxy复杂性
 **/
public interface MycatSessionView extends LocalInFileSession, PrepareStatementSession,
                                              MySQLServerSession<MycatSession> {

  default void proxyBackend(byte[] payload, String dataNodeName, boolean runOnSlave,
      LoadBalanceStrategy strategy, boolean noResponse,
      AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
    MycatSession mycat = (MycatSession) this;
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
    MycatDataNode dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(dataNodeName);
    MySQLProxyNIOHandler
        .INSTANCE
        .proxyHaBackend(mycat, bytes, (MySQLDataNode) dataNode, runOnSlave, strategy, noResponse,
            finallyCallBack);
  }

  default void proxyUpdateMultiBackends(byte[] payload,
      String masterDataNode,
      Collection<String> otherDataNode,
      AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
    MycatSession mycat = (MycatSession) this;
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
    MycatDataNode dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(masterDataNode);
    Iterator<String> iterator = otherDataNode.iterator();
    Collection<MySQLDataNode> mycatDataNodes = new ArrayList<>();
    while (iterator.hasNext()) {
      mycatDataNodes.add(ProxyRuntime.INSTANCE.getDataNodeByName(iterator.next()));
    }
    MySQLProxyNIOHandler
        .INSTANCE
        .proxyUpdateMultiBackends(mycat, bytes, (MySQLDataNode) dataNode, mycatDataNodes,
            finallyCallBack);
  }

  default void proxyUpdateMultiBackendsNoResponse(byte[] payload,
      String masterDataNode,
      Collection<String> otherDataNode,
      AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
    MycatSession mycat = (MycatSession) this;
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
    MycatDataNode dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(masterDataNode);
    Iterator<String> iterator = otherDataNode.iterator();
    Collection<MySQLDataNode> mycatDataNodes = new ArrayList<>();
    while (iterator.hasNext()) {
      mycatDataNodes.add(ProxyRuntime.INSTANCE.getDataNodeByName(iterator.next()));
    }
    MySQLProxyNIOHandler
        .INSTANCE
        .proxyUpdateMultiBackendsNoResponse(mycat, bytes, (MySQLDataNode) dataNode, mycatDataNodes,
            finallyCallBack);
  }
//  default boolean proxyBackend(byte[][] payloadList,String[] dataNodeName, boolean runOnSlaveList,
//      LoadBalanceStrategy strategy,
//      boolean noResponse, AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
//    assert dataNodeName != null && !"".equals(dataNodeName);
//    MycatSession mycat = (MycatSession) this;
//    MycatDataNode mycatDataNode = ProxyRuntime.INSTANCE
//                                      .getDataNodeByName(dataNodeName);
//    mycat.setCallBack(finallyCallBack);
//    boolean isMySQLDataNode = mycatDataNode instanceof MySQLDataNode;
//    mycat.setCallBack(finallyCallBack);
//    if (!isMySQLDataNode) {
//      mycat.setLastMessage("can not get mysql dataNode");
//      writeErrorEndPacket();
//      return false;
//    }
//    ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
//    if (proxyBuffer.channelReadEndIndex() > MySQLPacketSplitter.MAX_PACKET_SIZE) {
//      String message = "More than "
//                           + MySQLPacketSplitter.MAX_PACKET_SIZE
//                           + " so it can't be transmitted through";
//      mycat.setLastMessage(message);
//      writeErrorEndPacket();
//      return false;
//    }
//    proxyBuffer.channelWriteStartIndex(0);
//    proxyBuffer.channelWriteEndIndex(proxyBuffer.channelReadEndIndex());
//    mycat.getBackend(runOnSlave, (MySQLDataNode) mycatDataNode, strategy,
//        (mysql, sender, success, result, throwable) -> {
//          if (success) {
//            mycat.clearReadWriteOpts();
//            mycat.switchWriteHandler(MySQLProxySession.WriteHandler.INSTANCE);
//            mysql.setNoResponse(noResponse);
//            mysql.switchProxyNioHandler();
//            try {
//              mysql.writeProxyBufferToChannel(mycat.currentProxyBuffer());
//            } catch (IO_EXCEPTION e) {
//              String message = setLastMessage(e);
//              writeErrorEndPacket();
//            }
//            return;
//          } else {
//            writeErrorEndPacket();
//          }
//        });
//    return true;
//  }


  MycatSchema getSchema();

  void useSchema(MycatSchema schemaName);

  boolean hasResultset();

  boolean hasCursor();

  void countDownResultSet();

  void setResultSetCount(int count);

  byte[] encode(String text);

  void setAutoCommit(MySQLAutoCommit b);


  void setIsolation(MySQLIsolation isolation);

  void setMultiStatementSupport(boolean on);

  MycatUser getUser();

  void setCharset(String charset);

  void setCharsetSetResult(String charsetSetResult);

  boolean isBindMySQLSession();

  MySQLClientSession getMySQLSession();
}
