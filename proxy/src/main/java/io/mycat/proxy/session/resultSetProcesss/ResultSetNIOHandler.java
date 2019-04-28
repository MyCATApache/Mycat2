package io.mycat.proxy.session.resultSetProcesss;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.session.MycatSession;

import java.io.IOException;

public class ResultSetNIOHandler implements NIOHandler<MycatSession> {

    @Override
    public void onSocketRead(MycatSession session) throws IOException {

    }

    @Override
    public void onWriteFinished(MycatSession session) throws IOException {

    }

    @Override
    public void onSocketClosed(MycatSession session, boolean normal) {

    }
}
