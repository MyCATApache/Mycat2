/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.session.Session;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyReactorThread<T extends Session> extends Thread {
    protected final static long SELECTOR_TIMEOUT = 100;
    protected final FrontSessionManager<T> frontManager;

    public FrontSessionManager<T> getFrontManager() {
        return frontManager;
    }

    public Selector getSelector() {
        return selector;
    }

    protected final static Logger logger = LoggerFactory.getLogger(ProxyReactorThread.class);
    protected final Selector selector;
    protected final BufferPool bufPool;
    //用于管理连接等事件
    protected final ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<>();
    private final ReactorEnv reactorEnv = new ReactorEnv();

    @SuppressWarnings("unchecked")
    public ProxyReactorThread(BufferPool bufPool, FrontSessionManager<T> sessionMan) throws IOException {
        this.bufPool = bufPool;
        this.selector = Selector.open();
        this.frontManager = sessionMan;
    }

    /**
     * 处理连接请求
     * @param keyAttachement
     * @param socketChannel
     */
    public void acceptNewSocketChannel(Object keyAttachement, final SocketChannel socketChannel) {
        pendingJobs.offer(() -> {
            try {
                T sessionForConnectedChannel = frontManager.acceptNewSocketChannel(keyAttachement, this.bufPool, selector, socketChannel);
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("register new connection error " + e);
            }
        });
    }

    public BufferPool getBufPool() {
        return bufPool;
    }

    public void addNIOJob(Runnable job) {
        pendingJobs.offer(job);
    }

    private void processNIOJob() {
        Runnable nioJob = null;
        while ((nioJob = pendingJobs.poll()) != null) {
            try {
                nioJob.run();
            } catch (Exception e) {
                logger.warn("run nio job err ", e);
            }
        }
    }

    public ReactorEnv getReactorEnv() {
        return reactorEnv;
    }

    protected void processAcceptKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {

    }

    @SuppressWarnings("unchecked")
    protected void processConnectKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        T session = (T) curKey.attachment();
        reactorEnv.setCurSession(session);
        try {
            if (((SocketChannel) curKey.channel()).finishConnect()) {
                session.getCurNIOHandler().onConnect(curKey, session, true, null);
            }

        } catch (ConnectException ex) {
            session.getCurNIOHandler().onConnect(curKey, session, false, ex);
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
                    } catch (IOException e) {//如果设置为IOException方便调试,避免吞没其他类型异常
                        if (logger.isWarnEnabled()) {
                            logger.warn("Socket IO err :", e);
                        }
                        key.cancel();
                        if (reactorEnv.getCurSession() != null) {
                            reactorEnv.getCurSession().close(false, "Socket IO err:" + e);
                            reactorEnv.setCurSession(null);
                        }
                    }catch (Throwable t){
                        t.printStackTrace();
                        key.cancel();
                        if (reactorEnv.getCurSession() != null) {
                            reactorEnv.getCurSession().close(false, "logic  err:" + t);
                            reactorEnv.setCurSession(null);
                        }
                    }
                }
                keys.clear();
            } catch (IOException e) {
                logger.warn("catch error ", e);
            }
        }
    }
}
