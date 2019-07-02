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
package io.mycat.proxy.reactor;

import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import java.io.IOException;
import java.util.Objects;

/**
 * Mycat reactor 每个线程独立 一些同步的使用的,反复使用释放的帮助类对象可以放在此对象保存复用
 *
 * @author chen junwen
 */
public final class MycatReactorThread extends ProxyReactorThread<MycatSession> {

  private final MySQLSessionManager mySQLSessionManager;
  private final PacketSplitterImpl packetSplitter = new PacketSplitterImpl();
  private final ProxyRuntime runtime;

  public MycatReactorThread(BufferPool bufPool, FrontSessionManager<MycatSession> sessionManager,
      ProxyRuntime runtime)
      throws IOException {
    super(bufPool, sessionManager);
    this.runtime = runtime;
    this.mySQLSessionManager  = new MySQLSessionManager(runtime);
  }

  public PacketSplitterImpl getPacketSplitter() {
    return packetSplitter;
  }

  public MySQLSessionManager getMySQLSessionManager() {
    return mySQLSessionManager;
  }

  /**
   * Getter for property 'runtime'.
   *
   * @return Value for property 'runtime'.
   */
  public ProxyRuntime getRuntime() {
    return runtime;
  }


  public void close(Exception throwable) {
    super.close(throwable);
    try{
      Objects.requireNonNull(mySQLSessionManager);
      for (MySQLClientSession s : mySQLSessionManager.getAllSessions()) {
        mySQLSessionManager.removeSession(s,true,"close");
      }
    }catch (Exception e){
      LOGGER.error("{}",e);
    }
  }
}
