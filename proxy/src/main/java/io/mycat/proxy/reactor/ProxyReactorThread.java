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

import io.mycat.buffer.BufferPool;
import io.mycat.logTip.ReactorTip;
import io.mycat.proxy.handler.BackendNIOHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.session.Session;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reactor 任务调度,内存资源单位 无论是本线程内还是其他的线程,提交任务只能通过pendingQueue
 *
 * @author jamie12221
 *  date 2019-05-10 13:21
 **/
public abstract class ProxyReactorThread<T extends Session> extends Thread {

  /**
   * 定时唤醒selector的时间 1.防止写入事件得不到处理 2.处理pending队列
   */
  protected final static long SELECTOR_TIMEOUT = 100;
  protected final static Logger logger = LoggerFactory.getLogger(ProxyReactorThread.class);
  protected final FrontSessionManager<T> frontManager;
  protected final Selector selector;
  protected final BufferPool bufPool;
  //用于管理连接等事件
  protected final ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<>();
  private final ReactorEnv reactorEnv = new ReactorEnv();
  private static long activeTime = System.currentTimeMillis();

  @SuppressWarnings("unchecked")
  public ProxyReactorThread(BufferPool bufPool, FrontSessionManager<T> sessionMan)
      throws IOException {
    this.bufPool = bufPool;
    this.selector = Selector.open();
    this.frontManager = sessionMan;
  }

  public FrontSessionManager<T> getFrontManager() {
    return frontManager;
  }

  public Selector getSelector() {
    return selector;
  }

  /**
   * 处理连接请求 连接事件 连接事件由NIOAcceptor调用此函数 NIOAcceptor没有bufferpool
   */
  public void acceptNewSocketChannel(Object keyAttachement, final SocketChannel socketChannel) {
    assert Thread.currentThread() instanceof NIOAcceptor;
    pendingJobs.offer(() -> {
      try {
        frontManager
            .acceptNewSocketChannel(keyAttachement, this.bufPool,
                selector, socketChannel);
      } catch (Exception e) {
        logger.warn(ReactorTip.REGISTER_NEW_CONNECTION.getMessage(e));
      }
    });
  }

  /**
   * 向pending队列添加任务
   */
  public void addNIOJob(Runnable job) {
    pendingJobs.offer(job);
  }

  public BufferPool getBufPool() {
    return bufPool;
  }


  private void processNIOJob() {
    Runnable nioJob = null;
    while ((nioJob = pendingJobs.poll()) != null) {
      try {
        nioJob.run();
      } catch (Exception e) {
        logger.error(ReactorTip.PROCESS_NIO_JOB_EEROR.getMessage(e));
      }
    }
  }

  public ReactorEnv getReactorEnv() {
    return reactorEnv;
  }

  /**
   * 该方法仅NIOAcceptor使用
   */
  protected void processAcceptKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
    assert false;
  }

  /**
   * 该方法仅Reactor自身创建的主动连接使用
   */
  @SuppressWarnings("unchecked")
  protected void processConnectKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
    T session = (T) curKey.attachment();
    reactorEnv.setCurSession(session);
    SocketChannel channel = (SocketChannel) curKey.channel();
    NIOHandler curNIOHandler = session.getCurNIOHandler();
    if (curNIOHandler instanceof BackendNIOHandler) {
      BackendNIOHandler handler = (BackendNIOHandler) curNIOHandler;
      try {
        if (channel.finishConnect()) {
          handler.onConnect(curKey, session, true, null);
        }
      } catch (Exception e) {
        handler.onConnect(curKey, session, false, e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected void processReadKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
    T session = (T) curKey.attachment();
    reactorEnv.setCurSession(session);
    session.getCurNIOHandler().onSocketRead(session);
  }

  @SuppressWarnings("unchecked")
  protected void processWriteKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
    T session = (T) curKey.attachment();
    reactorEnv.setCurSession(session);
    session.getCurNIOHandler().onSocketWrite(session);
  }

  public void run() {
    long ioTimes = 0;

    while (true) {
      try {
        selector.select(SELECTOR_TIMEOUT);
        updateLastActiveTime();
        final Set<SelectionKey> keys = selector.selectedKeys();
        if (keys.isEmpty()) {
          if (!pendingJobs.isEmpty()) {
            ioTimes = 0;
            this.processNIOJob();
          }
          continue;
        } else if ((ioTimes > 5) & !pendingJobs.isEmpty()) {
          ioTimes = 0;
          this.processNIOJob();
        }
        ioTimes++;
        for (final SelectionKey key : keys) {
          try {
            int readdyOps = key.readyOps();
            reactorEnv.setCurSession(null);
            // 如果当前收到连接请求
            if ((readdyOps & SelectionKey.OP_ACCEPT) != 0) {
              processAcceptKey(reactorEnv, key);
            }
            // 如果当前连接事件
            else if ((readdyOps & SelectionKey.OP_CONNECT) != 0) {
              this.processConnectKey(reactorEnv, key);
            } else if ((readdyOps & SelectionKey.OP_READ) != 0) {
              this.processReadKey(reactorEnv, key);

            } else if ((readdyOps & SelectionKey.OP_WRITE) != 0) {
              this.processWriteKey(reactorEnv, key);
            }
          } catch (Exception e) {//如果设置为IOException方便调试,避免吞没其他类型异常
            logger.error("{}", e);
            Session curSession = reactorEnv.getCurSession();
            if (curSession != null) {
              NIOHandler curNIOHandler = curSession.getCurNIOHandler();
              if (curNIOHandler != null) {
                curNIOHandler.onException(curSession, e);
              } else {
                curSession.close(false, curSession.setLastMessage(e));
              }
              reactorEnv.setCurSession(null);
            }
          }
        }
        keys.clear();
      } catch (Throwable e) {
        logger.warn(ReactorTip.PROCESS_NIO_UNKNOWN_EEROR.getMessage(reactorEnv.getCurSession(), e));
      }
    }
  }

  /**
   * 获取该session,最近活跃的时间
   */
  public void updateLastActiveTime() {
    activeTime = System.currentTimeMillis();
  }

  public long getLastActiveTime() {
    return activeTime;
  }
}
