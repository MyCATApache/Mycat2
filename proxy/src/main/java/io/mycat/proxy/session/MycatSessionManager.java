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
package io.mycat.proxy.session;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.handler.CommandHandler;
import io.mycat.proxy.handler.MySQLClientAuthHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Supplier;

/**
 * 集中管理MySQL LocalInFileSession 是在mycat proxy中,唯一能够创建mysql session以及关闭mysqlsession的对象 该在一个线程单位里,对象生命周期应该是单例的
 *
 * @author jamie12221
 * @date 2019-05-10 13:21
 **/
public class MycatSessionManager implements FrontSessionManager<MycatSession> {

  final LinkedList<MycatSession> mycatSessions = new LinkedList<>();
  final Supplier<CommandHandler> commandHandlerFactory;

  public MycatSessionManager(
      Supplier<CommandHandler> commandHandlerFactory) {
    this.commandHandlerFactory = commandHandlerFactory;
  }


  @Override
  public Collection<MycatSession> getAllSessions() {
    return mycatSessions;
  }

  @Override
  public int currentSessionCount() {
    return mycatSessions.size();
  }

  /**
   * 调用该方法的时候 mycat session已经关闭了
   */
  @Override
  public void removeSession(MycatSession mycat, boolean normal, String reason) {
    try {
      MycatMonitor.onCloseMycatSession(mycat);
      mycatSessions.remove(mycat);
      mycat.channel().close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Override
  public MycatSession acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool,
      Selector nioSelector, SocketChannel frontChannel) throws IOException {
    MySQLClientAuthHandler mySQLClientAuthHandler = new MySQLClientAuthHandler();
    MycatSession mycat = new MycatSession(bufPool, nioSelector, frontChannel, SelectionKey.OP_READ,
        mySQLClientAuthHandler, this, commandHandlerFactory.get());
    mySQLClientAuthHandler.setMycatSession(mycat);
    mySQLClientAuthHandler.sendAuthPackge();
    MycatMonitor.onNewMycatSession(mycat);
    this.mycatSessions.add(mycat);
    return mycat;
  }
}
