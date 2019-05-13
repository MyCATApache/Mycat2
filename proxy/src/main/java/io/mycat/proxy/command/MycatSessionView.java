package io.mycat.proxy.command;

import io.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.command.LocalInFileRequestHandler.LocalInFileSession;
import io.mycat.proxy.command.PrepareStatementHandler.PrepareStatementSession;
import io.mycat.proxy.packet.MySQLPacketUtil;
import io.mycat.proxy.session.MySQLProxySession;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-12 22:41
 **/
public interface MycatSessionView extends LocalInFileSession, PrepareStatementSession,
                                              MySQLServerSession<MycatSession> {

  default boolean proxyBackend(byte[] payload, String dataNodeName, boolean runOnSlave,
      LoadBalanceStrategy strategy, boolean noResponse,
      AsynTaskCallBack<MycatSessionView> finallyCallBack) {
    MycatSession mycat = (MycatSession) this;
    mycat.resetProxyBuffer(MySQLPacketUtil.generateMySQLPacket(0, payload));
    return proxyBackend(dataNodeName, runOnSlave, strategy, noResponse, finallyCallBack);
  }

  default boolean proxyBackend(String dataNodeName, boolean runOnSlave,
      LoadBalanceStrategy strategy,
      boolean noResponse, AsynTaskCallBack<MycatSessionView> finallyCallBack) {
    assert dataNodeName != null && !"".equals(dataNodeName);
    MycatSession mycat = (MycatSession) this;
    MycatDataNode mycatDataNode = MycatRuntime.INSTANCE
                                      .getDataNodeByName(dataNodeName);
    mycat.setCallBack(finallyCallBack);
    boolean isMySQLDataNode = mycatDataNode instanceof MySQLDataNode;
    mycat.setCallBack(finallyCallBack);
    if (!isMySQLDataNode) {
      mycat.setLastMessage("can not get mysql dataNode");
      writeErrorEndPacket();
      return false;
    }
    ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
    if (proxyBuffer.channelReadEndIndex() > MySQLPacketSplitter.MAX_PACKET_SIZE) {
      String message = "More than "
                           + MySQLPacketSplitter.MAX_PACKET_SIZE
                           + " so it can't be transmitted through";
      mycat.setLastMessage(message);
      writeErrorEndPacket();
      return false;
    }
    proxyBuffer.channelWriteStartIndex(0);
    proxyBuffer.channelWriteEndIndex(proxyBuffer.channelReadEndIndex());
    mycat.getBackend(runOnSlave, (MySQLDataNode) mycatDataNode, strategy,
        (mysql, sender, success, result, throwable) -> {
          if (success) {
            mycat.clearReadWriteOpts();
            mycat.switchWriteHandler(MySQLProxySession.WriteHandler.INSTANCE);
            mysql.setNoResponse(noResponse);
            mysql.switchProxyNioHandler();
            try {
              mysql.writeProxyBufferToChannel(mycat.currentProxyBuffer());
            } catch (IOException e) {
              String message = setLastMessage(e);
              writeErrorEndPacket();
            }
            return;
          } else {
            writeErrorEndPacket();
          }
        });
    return true;
  }
}
