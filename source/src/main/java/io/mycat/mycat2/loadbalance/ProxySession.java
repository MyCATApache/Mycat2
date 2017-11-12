package io.mycat.mycat2.loadbalance;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.buffer.BufferPool;

/**
 * 负载均衡器后端会话
 * <p>
 * Created by ynfeng on 2017/9/13.
 */
public class ProxySession extends AbstractSession {
    private LBSession lbSession;

    public ProxySession(BufferPool bufferPool, Selector selector,
                        SocketChannel channel) throws IOException {
        super(bufferPool, selector, channel);
    }

    public void setCurBufOwner(boolean ownerFlag) {
        curBufOwner = ownerFlag;
    }

    public void giveupOwner(int intestOpts) {
        setCurBufOwner(false);
        clearReadWriteOpts();
        lbSession.setCurBufOwner(true);
        if (intestOpts == SelectionKey.OP_READ) {
            lbSession.change2ReadOpts();
        } else {
            lbSession.change2WriteOpts();
        }
    }

    public void takeOwner(int intestOpts) {
        curBufOwner = true;
        if (intestOpts == SelectionKey.OP_READ) {
            change2ReadOpts();
        } else {
            change2WriteOpts();
        }
        lbSession.setCurBufOwner(false);
        lbSession.clearReadWriteOpts();
    }

    @Override
    protected void doTakeReadOwner() {
        lbSession.takeOwner(SelectionKey.OP_READ);
    }

    public LBSession getLbSession() {
        return lbSession;
    }

    public void setLbSession(LBSession lbSession) {
        this.lbSession = lbSession;
    }
}
