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

import io.mycat.MycatException;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.Session;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 连接器
 *
 * @author chen junwen
 */
public final class NIOAcceptor extends ProxyReactorThread<Session> {

  ServerSocketChannel serverChannel;
  final ProxyRuntime runtime;

  public NIOAcceptor(BufferPool bufPool,ProxyRuntime runtime) throws IOException {
    super(bufPool, null);
    this.runtime = runtime;
  }

  protected void processAcceptKey(SelectionKey curKey) throws IOException {
    ServerSocketChannel serverSocket = (ServerSocketChannel) curKey.channel();
    // 接收通道，设置为非阻塞模式
    final SocketChannel socketChannel = serverSocket.accept();
    socketChannel.configureBlocking(false);
    LOGGER.info("New Client connected:{}", socketChannel);
    // Mycat fontchannel connect
    accept( socketChannel);

  }

  private void accept(SocketChannel socketChannel) throws IOException {
    // 找到一个可用的NIO Reactor Thread，交付托管
    MycatReactorThread[] reactorThreads = runtime.getMycatReactorThreads();
    MycatReactorThread nioReactor = reactorThreads[ThreadLocalRandom.current()
                                                       .nextInt(reactorThreads.length)];
    // 将通道注册到reactor对象上
    nioReactor.acceptNewSocketChannel(null, socketChannel);
  }

  public boolean startServerChannel(String ip, int port) throws IOException {
    openServerChannel(selector, ip, port);
    return true;
  }

  /**
   * 仅后台维护的主动创建的连接使用
   */
  @SuppressWarnings("unchecked")
  protected void processConnectKey(SelectionKey curKey) throws IOException {
    // only from cluster server socket
    SocketChannel curChannel = (SocketChannel) curKey.channel();
    Object obj = curKey.attachment();
    try {
      if (curChannel.finishConnect()) {
        throw new MycatException("unsupport!");
      }
    } catch (ConnectException ex) {
      LOGGER.warn("Connect failed:{}  message:{}", curChannel, ex);
    }
  }

  /**
   * 仅后台维护的主动创建的连接使用
   */
  @SuppressWarnings("unchecked")
  protected void processReadKey(SelectionKey curKey) throws IOException {
    // only from cluster server socket
    Session session = (Session) curKey.attachment();
    assert session != null;
    session.getCurNIOHandler().onSocketRead(session);
  }

  /**
   * 仅后台维护的主动创建的连接使用
   */
  protected void processWriteKey(SelectionKey curKey) throws IOException {
    // only from cluster server socket
    Session session = (Session) curKey.attachment();
    assert session != null;
    session.getCurNIOHandler().onSocketWrite(session);
  }

  /**
   * 仅后台维护的主动创建的连接使用
   */
  private void openServerChannel(Selector selector, String bindIp, int bindPort)
      throws IOException {
    serverChannel = ServerSocketChannel.open();
    final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
    serverChannel.bind(isa);
    serverChannel.configureBlocking(false);
    serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);
  }

  public Selector getSelector() {
    return this.selector;
  }

  @Override
  public void close(Exception e) {
    try {
      serverChannel.close();
    } catch (Exception e1) {
      LOGGER.warn("",e);
    }
    try {
      super.close(e);
    } catch (Exception e2) {
      LOGGER.warn("", e2);
    }
  }
}
