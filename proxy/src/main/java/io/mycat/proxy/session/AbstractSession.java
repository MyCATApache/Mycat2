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
package io.mycat.proxy.session;

import io.mycat.MycatExpection;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.buffer.ProxyBuffer;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public abstract class AbstractSession<T extends AbstractSession> implements Session<T> {
    protected final Selector nioSelector;
    protected long startTime;
    protected final SocketChannel channel;
    protected final SelectionKey channelKey;

    public SessionManager<? extends Session> getSessionManager() {
        return sessionManager;
    }

    protected long lastActiveTime;
    protected final SessionManager<? extends Session> sessionManager;
    protected NIOHandler nioHandler;

    public SelectionKey getChannelKey() {
        return channelKey;
    }

    protected final int sessionId;
    protected Throwable lastThrowable;

    public void switchNioHandler(NIOHandler nioHandler) {
        this.nioHandler = nioHandler;
    }

    public void switchDefaultNioHandler() {
        this.nioHandler = getSessionManager().getDefaultSessionHandler();
    }

    public void updateLastActiveTime(){
        lastActiveTime = System.currentTimeMillis();
    }

    public void switchDefaultNioHandler(Runnable runnable) {
        switchNioHandler(getSessionManager().getDefaultSessionHandler(), runnable);
    }

    public void switchNioHandler(NIOHandler nioHandler, Runnable runnable) {
        this.nioHandler = nioHandler;
        ((ProxyReactorThread) Thread.currentThread()).addNIOJob(runnable);
    }

    public AbstractSession(Selector selector, SocketChannel channel, int socketOpt, NIOHandler nioHandler, SessionManager<? extends Session> sessionManager) throws ClosedChannelException {
        this.nioSelector = selector;
        this.channel = channel;
        this.nioHandler = nioHandler;
        this.sessionManager = sessionManager;
        this.channelKey = channel.register(nioSelector, socketOpt, this);
        this.sessionId = MycatRuntime.INSTANCE.genSessionId();
        this.startTime = System.currentTimeMillis();
    }


    @Override
    public void setLastThrowable(Throwable e) {
        if (this.lastThrowable != null) {
            throw new MycatExpection("multi error！！！！");
        }
        this.lastThrowable = e;
    }

    @Override
    public boolean hasError() {
        return this.lastThrowable != null;
    }

    @Override
    public Throwable getLastThrowableAndReset() {
        Throwable lastThrowable = this.lastThrowable;
        this.lastThrowable = null;
        return lastThrowable;
    }

    @Override
    public String getLastThrowableInfoTextAndReset() {
        if (this.lastThrowable != null) {
            return lastThrowable.getMessage();
        } else {
            return null;
        }
    }
    public void change2ReadOpts() {
        channelKey.interestOps(SelectionKey.OP_READ);
        if (logger.isDebugEnabled())
            logger.debug("change to read opts {}", this);
    }

    public void clearReadWriteOpts() {
        this.channelKey.interestOps(0);
    }

    public void change2WriteOpts() {
        channelKey.interestOps(SelectionKey.OP_WRITE);
        if (logger.isDebugEnabled())
            logger.debug("change to write opts {}", this);
    }


    @Override
    public SocketChannel channel() {
        return channel;
    }

    @Override
    public boolean isClosed() {
        return !channel.isOpen();
    }

    @Override
    public NIOHandler getCurNIOHandler() {
        return nioHandler;
    }

    @Override
    public int sessionId() {
        return sessionId;
    }

}
