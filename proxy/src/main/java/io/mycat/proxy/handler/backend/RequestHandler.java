/**
 * Copyright (C) <2020>  <jamie12221>
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
package io.mycat.proxy.handler.backend;

import io.mycat.MycatException;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 *  date 2019-05-22 11:13
 **/
public enum RequestHandler implements NIOHandler<MySQLClientSession> {
  INSTANCE;
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandler.class);
  public void request(MySQLClientSession session, byte[] packet, RequestCallback callback) {
    session.setCallBack(callback);
    try {
      session.setCallBack(callback);
      session.switchNioHandler(this);
      MycatReactorThread thread = (MycatReactorThread)Thread.currentThread();
      session.setCurrentProxyBuffer(new ProxyBufferImpl(thread.getBufPool()));
      session.writeProxyBufferToChannel(packet);
    } catch (Exception e) {
      MycatMonitor.onRequestException(session,e);
      onException(session, e);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onSocketRead(MySQLClientSession session) {
    Exception e = new MycatException("unknown read data");
    try {
      session.readFromChannel();
    } catch (Exception e1) {
      e = e1;
    }
    MycatMonitor.onRequestException(session,e);
    RequestCallback callback = session.getCallBack();
    onException(session, e);
    callback.onFinishedSendException(e, this, null);
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {
    try {
      session.writeToChannel();
    } catch (Exception e) {
      MycatMonitor.onRequestException(session,e);
      RequestCallback callback = session.getCallBack();
      onException(session, e);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    RequestCallback callback = session.getCallBack();
    onClear(session);
    callback.onFinishedSend(session, this, null);
  }

  @Override
  public void onException(MySQLClientSession session, Exception e) {
    MycatMonitor.onRequestException(session,e);
    LOGGER.error("{}", e);
    onClear(session);
    session.setCallBack(null);
    session.close(false, e);
  }

  public void onClear(MySQLClientSession session) {
    session.resetPacket();
    session.setCallBack(null);
    session.switchNioHandler(null);
    MycatMonitor.onRequestClear(session);
  }
}
