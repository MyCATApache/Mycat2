/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 实际包含运行状态的session实现 本对象封装 1.selector 2.读写通道 4.session创建时间 sessionId就是connectionId
 *
 * @param <T> 子类
 * @author jamie12221 date 2019-05-10 13:21
 */
public abstract class AbstractSession<T extends AbstractSession> implements Session<T> {

    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(AbstractSession.class);
    protected SocketChannel channel;
    protected SelectionKey channelKey;
    protected final SessionManager<T> sessionManager;
    protected final int sessionId;
    protected long startTime;
    protected long lastActiveTime;
    protected NIOHandler nioHandler;
    protected boolean hasClosed = false;
    private final MycatReactorThread ioThread;

    public AbstractSession(int sessionId,
                           NIOHandler nioHandler, SessionManager<T> sessionManager) {
        this.ioThread = (MycatReactorThread) Thread.currentThread();
        this.nioHandler = nioHandler;
        this.sessionManager = sessionManager;
        this.sessionId = sessionId;
        this.startTime = currentTimeMillis();
    }

    public void register(Selector selector, SocketChannel channel, int socketOpt)
            throws ClosedChannelException {
        this.channel = channel;
        this.channelKey = channel.register(selector, socketOpt, this);
    }

    public SelectionKey getChannelKey() {
        return channelKey;
    }

    /**
     *
     */
    abstract public void switchNioHandler(NIOHandler nioHandler);


    public void updateLastActiveTime() {
        lastActiveTime = currentTimeMillis();
    }

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    public void change2ReadOpts() {
        if ((channelKey.interestOps() & SelectionKey.OP_READ) == 0) {
            channelKey.interestOps(SelectionKey.OP_READ);
            MycatMonitor.onChange2ReadOpts(this);
        }
    }

    public void change2WriteOpts() {
        if ((channelKey.interestOps() & SelectionKey.OP_WRITE) == 0) {
            channelKey.interestOps(SelectionKey.OP_WRITE);
            MycatMonitor.onChange2WriteOpts(this);
        }
    }

    public void clearReadWriteOpts() {
        if (channelKey.interestOps() != 0) {
            channelKey.interestOps(0);
            MycatMonitor.onClearReadWriteOpts(this);
        }
    }

    @Override
    public SocketChannel channel() {
        return channel;
    }

    @Override
    public boolean hasClosed() {
        return hasClosed;
    }

    @Override
    public NIOHandler getCurNIOHandler() {
        return nioHandler;
    }

    @Override
    public int sessionId() {
        return sessionId;
    }

    public SessionManager<T> getSessionManager() {
        return sessionManager;
    }

    public boolean checkOpen() {
        SocketChannel channel = channel();
        boolean open = !hasClosed() && channel.isOpen() && channel.isConnected();
        if (open) {
            ByteBuffer allocate = ByteBuffer.allocate(0);

            boolean close;
            try {
                close = -1 == channel.read(allocate);
            } catch (IOException e) {
                close = true;
            }
            if (close){
                this.close(false,"check open");
                return false;
            }
        }
        return true;
    }

    @Override
    public MycatReactorThread getIOThread() {
        return ioThread;
    }
}
