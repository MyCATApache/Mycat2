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
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MySQLClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum IdleHandler implements NIOHandler<MySQLClientSession> {
  INSTANCE;
  private static final Logger LOGGER = LoggerFactory.getLogger(IdleHandler.class);

  @Override
  public void onSocketRead(MySQLClientSession session) {
    MycatMonitor.onIdleReadException(session,
        new MycatException("mysql session:{} is idle but it receive response", session));
    session.close(false, "mysql session  is idle but it receive response");
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {

  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    assert false;
  }

  @Override
  public void onException(MySQLClientSession session, Exception e) {
    LOGGER.error("{}", e);
    session.close(false, e);
  }

}