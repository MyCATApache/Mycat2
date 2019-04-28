package io.mycat.proxy.session;

import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.MainMycatNIOHandler;
import io.mycat.proxy.handler.MySQLClientAuthHandler;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;

public class MycatSessionManager implements FrontSessionManager<MycatSession> {
   final LinkedList<MycatSession> mycatSessions = new LinkedList<>();
    @Override
    public Collection<MycatSession> getAllSessions() {
        return null;
    }

    @Override
    public int curSessionCount() {
        return mycatSessions.size();
    }

    @Override
    public NIOHandler<MycatSession> getDefaultSessionHandler() {
        return MainMycatNIOHandler.INSTANCE;
    }

    @Override
    public void removeSession(MycatSession mycat) {
        mycatSessions.remove(mycat);
    }

    @Override
    public MycatSession acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException {
        if(!frontChannel.isConnected()){
            throw new MycatExpection("");
        }
        MySQLClientAuthHandler mySQLClientAuthHandler = new MySQLClientAuthHandler();
        MycatSession mycat = new MycatSession(bufPool, nioSelector,frontChannel, SelectionKey.OP_READ,mySQLClientAuthHandler,this);
        mySQLClientAuthHandler.setMycatSession(mycat);
        mySQLClientAuthHandler.sendAuthPackge();
        this.mycatSessions.add(mycat);
        return mycat;
    }
}
