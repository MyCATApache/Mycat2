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

import static io.mycat.logTip.SessionTip.UNKNOWN_IDLE_RESPONSE;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatMonitor;
import io.mycat.proxy.MycatSessionView;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketUtil;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.task.client.MultiMySQLUpdateNoResponseTask;
import io.mycat.proxy.task.client.MultiMySQLUpdateTask;
import io.mycat.proxy.task.client.MySQLTaskUtil;
import java.io.IOException;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public enum MySQLPacketExchanger {
  INSTANCE;

  public static void clear(MycatSession mycatSession, MySQLClientSession mysql) {
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
  }

  public void onBackendResponse(MySQLClientSession mysql) throws IOException {
    if (!mysql.readFromChannel()) {
      return;
    }
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
    mysql.getMycatSession().writeToChannel();

    return;
  }


  public boolean onBackendWriteFinished(MySQLClientSession mysql) {
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

  public boolean onFrontWriteFinished(MycatSession mycat) {
    MySQLClientSession mysql = mycat.getMySQLSession();
    if (mysql.isResponseFinished()) {
      mycat.change2ReadOpts();
      return true;
    } else {
      mysql.change2ReadOpts();
      ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
      int writeEndIndex = proxyBuffer.channelWriteEndIndex();
      proxyBuffer.channelReadStartIndex(writeEndIndex);
      return false;
    }
  }

  public enum ResultType {
    SUCCESS,
    REQUEST_ERROR,
    OTHER_ERROR
  }

  public enum MySQLProxyNIOHandler implements NIOHandler<MySQLClientSession> {
    INSTANCE;


    protected final static Logger logger = LoggerFactory.getLogger(MySQLProxyNIOHandler.class);
    static final MySQLPacketExchanger HANDLER = MySQLPacketExchanger.INSTANCE;

    public void proxyUpdateMultiBackends(MycatSession mycat, byte[] bytes,
        MySQLDataNode masterDataNode,
        Collection<MySQLDataNode> otherDataNode,
        AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
      new MultiMySQLUpdateTask(mycat, bytes, otherDataNode,
          (session, sender, success, result, attr) -> {
            proxyBackend(mycat, bytes,
                masterDataNode, false, null, false,
                finallyCallBack);
          });
    }

    public void proxyUpdateMultiBackendsNoResponse(MycatSession mycat, byte[] payload,
        MySQLDataNode masterDataNode,
        Collection<MySQLDataNode> otherDataNode,
        AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
      byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
      new MultiMySQLUpdateNoResponseTask(mycat, bytes, otherDataNode,
          (session, sender, success, result, attr) -> {
            proxyBackend(mycat, bytes,
                masterDataNode, false, null, false,
                finallyCallBack);
          });
    }

    public void proxyHaBackend(MycatSession mycat, byte[] bytes, MySQLDataNode dataNode,
        boolean runOnSlave,
        LoadBalanceStrategy strategy,
        boolean noResponse, AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
      mycat.currentProxyBuffer().reset();
      proxyBackend(mycat, bytes, dataNode, runOnSlave, strategy, noResponse,
          new AsyncTaskCallBack<MycatSessionView>() {
            byte counter = 3;

            @Override
            public void finished(MycatSessionView session, Object sender, boolean success,
                Object result, Object attr) {
              if (result == ResultType.SUCCESS) {
                finallyCallBack.finished(session, sender, success, result, attr);
                return;
              } else if (--counter != 0) {
                proxyBackend(mycat, bytes, dataNode, runOnSlave, strategy, noResponse,
                    this);
                return;
              }
              finallyCallBack.finished(session, sender, success, result, attr);
            }
          });

    }

    public void proxyBackend(MycatSession mycat, byte[] bytes, MySQLDataNode dataNode,
        boolean runOnSlave,
        LoadBalanceStrategy strategy,
        boolean noResponse, AsyncTaskCallBack<MycatSessionView> finallyCallBack) {
      mycat.currentProxyBuffer().reset();
      mycat.setCallBack(finallyCallBack);
      getBackend(mycat, runOnSlave, dataNode, strategy,
          (mysql, sender, success, result, attr) -> {
            AsyncTaskCallBack<MycatSessionView> callBack = mycat.getCallBack();
            if (success) {
              mysql.setNoResponse(noResponse);
              mysql.switchNioHandler(this);
              mycat.setMySQLSession(mysql);
              mycat.switchWriteHandler(WriteHandler.INSTANCE);
              mycat.currentProxyBuffer().newBuffer(bytes);
              try {
                mysql.writeProxyBufferToChannel(mycat.currentProxyBuffer());
                mycat.setMySQLSession(mysql);
                mysql.setMycatSession(mycat);
                MycatMonitor.onBindMySQLSession(mycat, mysql);
              } catch (IOException e) {
                String message = mycat.setLastMessage(e);
                mysql.close(false, message);
                callBack.finished(mycat, this, false, ResultType.REQUEST_ERROR, null);
              }
              return;
            } else {
              mycat.setLastMessage((String) result);
              callBack.finished(mycat, this, false, ResultType.REQUEST_ERROR, null);
            }
          });
    }

    public void getBackend(MycatSession mycat, boolean runOnSlave, MySQLDataNode dataNode,
        LoadBalanceStrategy strategy, AsyncTaskCallBack<MySQLClientSession> finallyCallBack) {
      mycat.switchDataNode(dataNode.getName());
      if (mycat.getMySQLSession() != null) {
        //只要backend还有值,就说明上次命令因为事务或者遇到预处理,loadata这种跨多次命令的类型导致mysql不能释放
        finallyCallBack.finished(mycat.getMySQLSession(), this, true, null, null);
        return;
      }
      MySQLIsolation isolation = mycat.getIsolation();
      MySQLAutoCommit autoCommit = mycat.getAutoCommit();
      String charsetName = mycat.getCharsetName();
      MySQLTaskUtil
          .getMySQLSession(dataNode, isolation, autoCommit, charsetName,
              runOnSlave,
              strategy, finallyCallBack);
    }

    @Override
    public void onSocketRead(MySQLClientSession mysql) throws IOException {
      try {
        HANDLER.onBackendResponse(mysql);
        mysql.setRequestSuccess(true);
      } catch (Throwable e) {
        MycatSession mycat = mysql.getMycatSession();
        AsyncTaskCallBack<MycatSessionView> callBack = mycat.getCallBack();
        mycat.setCallBack(null);
        String message = mycat.setLastMessage(e);
        mysql.close(false, message);
        if (mysql.isRequestSuccess()) {
          callBack.finished(mycat, this, false, ResultType.OTHER_ERROR, null);
          return;
        } else {
          callBack.finished(mycat, this, false, ResultType.REQUEST_ERROR, null);
          return;
        }
      }
    }

    @Override
    public void onWriteFinished(MySQLClientSession session) {
      boolean b = HANDLER.onBackendWriteFinished(session);
      session.setRequestSuccess(false);
      if (b) {
        MycatSession mycatSession = session.getMycatSession();
        clear(mycatSession, session);
        mycatSession.onHandlerFinishedClear(true);
        AsyncTaskCallBack<MycatSessionView> callBack = mycatSession.getCallBack();
        mycatSession.setCallBack(null);
        callBack.finished(mycatSession, this, true, ResultType.SUCCESS, null);
      }
    }


    @Override
    public void onSocketClosed(MySQLClientSession session, boolean normal, String reason) {
      clear(session.getMycatSession(), session);
    }
  }

  public enum MySQLIdleNIOHandler implements NIOHandler<MySQLClientSession> {
    INSTANCE;
    protected final static Logger logger = LoggerFactory.getLogger(
        MySQLPacketExchanger.MySQLProxyNIOHandler.class);

    @Override
    public void onSocketRead(MySQLClientSession session) throws IOException {
      session.close(false, UNKNOWN_IDLE_RESPONSE.getMessage());
    }

    @Override
    public void onWriteFinished(MySQLClientSession session) throws IOException {
      assert false;
    }


    /**
     * 因为onSocketClosed是被session.close调用,所以不需要重复调用
     */
    @Override
    public void onSocketClosed(MySQLClientSession session, boolean normal, String reason) {

    }
  }

  /**
   * 代理模式前端写入处理器
   */
  enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession mycat) throws IOException {
      ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
      int oldIndex = proxyBuffer.channelWriteStartIndex();
      proxyBuffer.writeToChannel(mycat.channel());

      MycatMonitor.onFrontWrite(mycat, proxyBuffer.currentByteBuffer(), oldIndex,
          proxyBuffer.channelReadEndIndex());
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
            clear(mycat, mysql);
            mycat.onHandlerFinishedClear(true);
          }
        }
      }
    }

    /**
     * mycat seesion没有重写onWriteFinished方法,所以onWriteFinished调用的是此类的writeToChannel方法
     */
    @Override
    public void onWriteFinished(MycatSession proxySession) throws IOException {
      proxySession.writeFinished(proxySession);
    }
  }
}
