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
import io.mycat.proxy.handler.MySQLClientAuthHandler;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;

/**
 * 集中管理MySQL Session 是在mycat proxy中,唯一能够创建mysql session以及关闭mysqlsession的对象 该在一个线程单位里,对象生命周期应该是单例的
 *
 * @author jamie12221
 * @date 2019-05-10 13:21
 **/
public class MycatSessionManager implements FrontSessionManager<MycatSession> {

  final LinkedList<MycatSession> mycatSessions = new LinkedList<>();

  @Override
  public Collection<MycatSession> getAllSessions() {
    return mycatSessions;
  }

  @Override
  public int currentSessionCount() {
    return mycatSessions.size();
  }

  @Override
  public void removeSession(MycatSession mycat, boolean normal, String reason) {
    mycatSessions.remove(mycat);
//    mycat.close(normal, reason);
  }


  @Override
  public MycatSession acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool,
      Selector nioSelector, SocketChannel frontChannel) throws IOException {
    MySQLClientAuthHandler mySQLClientAuthHandler = new MySQLClientAuthHandler();
    MycatSession mycat = new MycatSession(bufPool, nioSelector, frontChannel, SelectionKey.OP_READ,
        mySQLClientAuthHandler, this);
    mySQLClientAuthHandler.setMycatSession(mycat);
    mySQLClientAuthHandler.sendAuthPackge();
    this.mycatSessions.add(mycat);
    return mycat;
  }
}
