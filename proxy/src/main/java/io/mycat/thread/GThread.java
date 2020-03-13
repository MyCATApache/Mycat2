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
package io.mycat.thread;

import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadPool;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.Session;
/**
 * @author Junwen Chen
 **/
public class GThread extends BindThread implements SessionThread {

  protected Session session;

  private static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(GThread.class);

  public GThread( BindThreadPool manager) {
    super(manager);
  }


  @Override
  public Session getCurSession() {
    return session;
  }

  @Override
  public void setCurSession(Session session) {
    this.session = session;
  }
}