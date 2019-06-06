/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.handler;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.callback.TaskCallBack;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public enum MySQLPacketExchanger {
  INSTANCE;

  private static final Logger logger = LoggerFactory.getLogger(MySQLPacketExchanger.class);
  public final static PacketExchangerCallback DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK = (mycat, e, attr) -> {
    mycat.setLastMessage(e.getMessage());
    mycat.writeErrorEndPacketBySyncInProcessError();
  };

  private static void onExceptionClearCloseInResponse(MycatSession mycat, Exception e) {
    logger.error("{}", e);
    MycatMonitor.onPacketExchangerException(mycat, e);
    MySQLClientSession mysql = mycat.getMySQLSession();
    if (mysql != null) {
      mysql.resetPacket();
      mysql.setCallBack(null);
      mysql.close(false, e);
      mycat.onHandlerFinishedClear();
    }
    mycat.close(false, e);
  }

  private static void onExceptionClearCloseInRequest(MycatSession mycat, Exception e,
      PacketExchangerCallback callbac) {
    logger.error("{}", e);
    MycatMonitor.onPacketExchangerWriteException(mycat, e);
    MySQLClientSession mysql = mycat.getMySQLSession();
    if (mysql != null) {
      mysql.setCallBack(null);
      mysql.resetPacket();
      mysql.close(false, e);
    }
    callbac.onRequestMySQLException(mycat, e, null);
  }

  private static void onClearInNormalResponse(MycatSession mycatSession, MySQLClientSession mysql) {
    mycatSession.resetPacket();
    mysql.resetPacket();
    mysql.setNoResponse(false);

    if (!mysql.isMonopolized()) {
      mycatSession.setMySQLSession(null);
      mysql.setMycatSession(null);
      MycatMonitor.onUnBindMySQLSession(mycatSession, mysql);
      mysql.switchNioHandler(null);
      mysql.getSessionManager().addIdleSession(mysql);
    }
    mycatSession.onHandlerFinishedClear();
    MycatMonitor.onPacketExchangerClear(mycatSession);
  }

  public void proxyBackend(MycatSession mycat, byte[] payload, String dataNodeName,
      boolean runOnMaster,
      LoadBalanceStrategy strategy, boolean noResponse) {
    proxyBackend(mycat, payload, dataNodeName, runOnMaster, strategy, noResponse,
        DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK);

  }

  public void proxyBackend(MycatSession mycat, byte[] payload, String dataNodeName,
      boolean runOnMaster,
      LoadBalanceStrategy strategy, boolean noResponse, PacketExchangerCallback finallyCallBack) {
    byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
    MycatDataNode dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(dataNodeName);
    MySQLProxyNIOHandler
        .INSTANCE.proxyBackend(mycat, bytes, (MySQLDataNode) dataNode, runOnMaster, strategy,
        noResponse, finallyCallBack
    );
  }

  private void onBackendResponse(MySQLClientSession mysql) throws IOException {
    MycatSession mycatSession = mysql.getMycatSession();
    if (!mysql.readFromChannel()) {
      return;
    }
    mysql.setRequestSuccess(true);
    MycatMonitor.onPacketExchangerRead(mysql);
    ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
    MySQLPacketResolver packetResolver = mysql.getPacketResolver();
    int startIndex = mySQLPacket.packetReadStartIndex();
    int endPos = startIndex;
    while (mysql.readPartProxyPayload()) {
      endPos = packetResolver.getEndPos();
      mySQLPacket.packetReadStartIndex(endPos);
    }
    proxyBuffer.channelWriteStartIndex(startIndex);
    proxyBuffer.channelWriteEndIndex(endPos);

    mycatSession.writeToChannel();
    return;
  }

  private boolean onBackendWriteFinished(MySQLClientSession mysql) {
    if (!mysql.isNoResponse()) {
      ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
      proxyBuffer.channelReadStartIndex(0);
      proxyBuffer.channelReadEndIndex(0);
      mysql.prepareReveiceResponse();
      mysql.change2ReadOpts();
      return false;
    } else {
      return true;
    }
  }

  private boolean onFrontWriteFinished(MycatSession mycat) {
    MySQLClientSession mysql = mycat.getMySQLSession();
    ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
    if (proxyBuffer.channelWriteFinished() && mysql.isResponseFinished()) {
      mycat.change2ReadOpts();
      return true;
    } else {
      mysql.change2ReadOpts();
      int writeEndIndex = proxyBuffer.channelWriteEndIndex();
      proxyBuffer.channelReadStartIndex(writeEndIndex);
      return false;
    }
  }

  private enum MySQLProxyNIOHandler implements BackendNIOHandler<MySQLClientSession> {
    INSTANCE;


    protected final static Logger logger = LoggerFactory.getLogger(MySQLProxyNIOHandler.class);
    static final MySQLPacketExchanger HANDLER = MySQLPacketExchanger.INSTANCE;

    public static void getBackend(MycatSession mycat, boolean runOnMaster, MySQLDataNode dataNode,
        LoadBalanceStrategy strategy, SessionCallBack<MySQLClientSession> finallyCallBack) {
      mycat.switchDataNode(dataNode.getName());
      if (mycat.getMySQLSession() != null) {
        //只要backend还有值,就说明上次命令因为事务或者遇到预处理,loadata这种跨多次命令的类型导致mysql不能释放
        finallyCallBack.onSession(mycat.getMySQLSession(), MySQLPacketExchanger.INSTANCE, null);
        return;
      }
      MySQLIsolation isolation = mycat.getIsolation();
      MySQLAutoCommit autoCommit = mycat.getAutoCommit();
      String charsetName = mycat.getCharsetName();
      String characterSetResult = mycat.getCharacterSetResults();
      MySQLTaskUtil
          .getMySQLSession(dataNode, isolation, autoCommit, charsetName, characterSetResult,
              runOnMaster,
              strategy, finallyCallBack);
    }

    public void proxyBackend(MycatSession mycat, byte[] bytes, MySQLDataNode dataNode,
        boolean runOnMaster,
        LoadBalanceStrategy strategy,
        boolean noResponse, PacketExchangerCallback finallyCallBack) {
      mycat.currentProxyBuffer().reset();
      getBackend(mycat, runOnMaster, dataNode, strategy, new SessionCallBack<MySQLClientSession>() {
        @Override
        public void onSession(MySQLClientSession mysql, Object sender, Object attr) {
          try {
            mysql.setCallBack(finallyCallBack);
            mysql.setNoResponse(noResponse);
            mysql.switchNioHandler(MySQLProxyNIOHandler.INSTANCE);
            mycat.setMySQLSession(mysql);
            mycat.switchWriteHandler(WriteHandler.INSTANCE);
            mycat.currentProxyBuffer().newBuffer(bytes);
            mysql.writeProxyBufferToChannel(mycat.currentProxyBuffer());
            mycat.setMySQLSession(mysql);
            mysql.setMycatSession(mycat);
            MycatMonitor.onBindMySQLSession(mycat, mysql);
          } catch (Exception e) {
            onExceptionClearCloseInRequest(mycat, e, finallyCallBack);
            return;
          }
        }

        @Override
        public void onException(Exception exception, Object sender, Object attr) {
          MycatMonitor.onGettingBackendException(mycat, dataNode.getName(), exception);
          finallyCallBack.onRequestMySQLException(mycat, exception, attr);
        }
      });
    }

    @Override
    public void onSocketRead(MySQLClientSession mysql) {
      try {
        HANDLER.onBackendResponse(mysql);
      } catch (Exception e) {
        MycatSession mycat = mysql.getMycatSession();
        if (mysql.isRequestSuccess()) {
          onExceptionClearCloseInResponse(mycat, e);
          return;
        } else {
          onExceptionClearCloseInRequest(mycat, e, mysql.getCallBack());
          return;
        }
      }
    }

    @Override
    public void onSocketWrite(MySQLClientSession session) {
      try {
        session.writeToChannel();
        MycatMonitor.onPacketExchangerWrite(session);
      } catch (Exception e) {
        onExceptionClearCloseInResponse(session.getMycatSeesion(), e);
      }
    }

    @Override
    public void onWriteFinished(MySQLClientSession session) {
      boolean b = HANDLER.onBackendWriteFinished(session);
      session.setRequestSuccess(false);
      if (b) {
        MycatSession mycatSession = session.getMycatSession();
        onClearInNormalResponse(mycatSession, session);
      }
    }

    @Override
    public void onException(MySQLClientSession session, Exception e) {
      MycatSession mycatSeesion = session.getMycatSeesion();
      onExceptionClearCloseInResponse(mycatSeesion, e);
    }
  }


  /**
   * 代理模式前端写入处理器
   */
  private enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession mycat) throws IOException {
      try {
        mycat.getMySQLSession().clearReadWriteOpts();
        ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
        int oldIndex = proxyBuffer.channelWriteStartIndex();
        int endIndex = proxyBuffer.channelWriteEndIndex();
        MycatMonitor.onPacketExchangerWrite(mycat);
        proxyBuffer.writeToChannel(mycat.channel());

        MycatMonitor.onFrontWrite(mycat, proxyBuffer.currentByteBuffer(), oldIndex,
            endIndex - oldIndex);
        mycat.updateLastActiveTime();

        if (!proxyBuffer.channelWriteFinished()) {
          mycat.change2WriteOpts();
        } else {
          MySQLClientSession mysql = mycat.getMySQLSession();
          if (mysql == null) {
            assert false;
          } else {
            boolean b = MySQLPacketExchanger.INSTANCE.onFrontWriteFinished(mycat);
            if (b) {
              onClearInNormalResponse(mycat, mysql);
            }
          }
        }
      } catch (Exception e) {
        onExceptionClearCloseInResponse(mycat, e);
      }
    }

    @Override
    public void onException(MycatSession session, Exception e) {
      onExceptionClearCloseInResponse(session, e);
    }
  }

  public interface PacketExchangerCallback extends TaskCallBack<PacketExchangerCallback> {

    void onRequestMySQLException(MycatSession mycat, Exception e, Object attr);
  }
}
