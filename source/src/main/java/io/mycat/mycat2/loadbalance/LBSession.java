package io.mycat.mycat2.loadbalance;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;

/**
 * 负载均衡器前端会话
 * <p>
 * Created by ynfeng on 2017/9/13.
 */
public class LBSession extends AbstractSession {
    private ProxySession proxySession;

    public LBSession(BufferPool bufferPool, Selector selector,
                     SocketChannel channel, NIOHandler nioHandler) throws IOException {
        super(bufferPool, selector, channel,nioHandler);
    }

    public void setCurBufOwner(boolean ownerFlag) {
        curBufOwner = ownerFlag;
    }

    public void takeOwner(int intestOpts) {
        curBufOwner = true;
        if (intestOpts == SelectionKey.OP_READ) {
            change2ReadOpts();
        } else {
            change2WriteOpts();
        }
        proxySession.setCurBufOwner(false);
        proxySession.clearReadWriteOpts();
    }

    public void giveupOwner(int intestOpts) {
        setCurBufOwner(false);
        clearReadWriteOpts();
        proxySession.setCurBufOwner(true);
        if (intestOpts == SelectionKey.OP_READ) {
            proxySession.change2ReadOpts();
        } else {
            proxySession.change2WriteOpts();
        }
    }

    @Override
    protected void doTakeReadOwner() {
        proxySession.takeOwner(SelectionKey.OP_READ);
    }

    public ProxySession getProxySession() {
        return proxySession;
    }

    public void setProxySession(ProxySession proxySession) {
        this.proxySession = proxySession;
    }
}
