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

import io.mycat.beans.DataNode;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferWriteIter;
import io.mycat.proxy.packet.ComQueryState;
import io.mycat.proxy.task.MySQLAPI;
import io.mycat.replica.Datasource;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class MySQLSession extends AbstractMySQLSession implements MySQLAPI {
    public MySQLSession(Datasource datasource, Selector selector, SocketChannel channel, int socketOpt, NIOHandler nioHandler, MySQLSessionManager sessionManager) throws IOException {
        super(selector, channel, socketOpt, nioHandler, sessionManager);
        this.datasource = datasource;
    }

    private MycatSession mycat;
    private ProxyBuffer proxyBuffer;
    private final Datasource datasource;


    public DataNode getDataNode() {
        return dataNode;
    }

    public void setDataNode(DataNode dataNode) {
        this.dataNode = dataNode;
    }

    private DataNode dataNode;


    public Datasource getDatasource(){
        return datasource;
    }

    public void setProxyBuffer(ProxyBuffer proxyBuffer) {
        this.proxyBuffer = proxyBuffer;
    }

    public MycatSession getMycatSession() {
        return mycat;
    }

    public void writeToChannel(ProxyBuffer proxyBuffer) throws IOException {
        this.proxyBuffer = proxyBuffer;
        this.writeToChannel();
    }

    @Override
    public void close(boolean normal, String hint) {
        //proxyBuffer.reset();MySQLSession不能释放Proxybuffer,proxybuffer是mycatSession分配的
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ProxyBuffer currentProxyBuffer() {
        return proxyBuffer;
    }

    public void bind(MycatSession mycatSession) {
        this.mycat = mycatSession;
        mycatSession.bind(this);
    }

    public void prepareReveiceResponse() {
        this.packetResolver.setState(ComQueryState.FIRST_PACKET);
    }
    public void prepareReveicePrepareOkResponse() {
        this.packetResolver.setState(ComQueryState.FIRST_PACKET);
        this.packetResolver.setCurrentComQuerySQLType(0x22);
    }
    public boolean unbindMycatIfNeed(MycatSession mycat) {
        this.resetPacket();
        this.proxyBuffer = null;
        mycat.resetPacket();
        MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
        reactorThread.getMySQLSessionManager().addIdleSession(this);
        return true;
    }
    public boolean end() {
        this.resetPacket();
        this.proxyBuffer = null;
        if (mycat!=null){
            mycat.resetPacket();
        }
        MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
        reactorThread.getMySQLSessionManager().addIdleSession(this);
        return true;
    }
    public void synchronizedState(DataNode dataNode, AsynTaskCallBack<MySQLSession> finallyCallBack) {

    }

    public void prepareOk(String s, MySQLPacketCallback callback) {
    }

    @Override
    public MySQLSession getThis() {
        return this;
    }
//
//    public boolean isSynchronizedState(String targetDatabase, StringBuilder str) {
//        MySQLMeta mysqlMeta = getMysqlMeta();
//        MycatSession mycat = getMycatSession();
//        str.setLength(0);
//        switch (mysqlMeta.getType()) {
//            case MASTER_NODE:
//                if (!mycat.getIsolation().equals(this.getIsolation())) {
//                    str.append(mycat.getIsolation().getCmd());
//                    str.append(";");
//                    return false;
//                }
//                if (!mycat.getAutoCommit().equals(this.getAutoCommit())) {
//                    str.append(mycat.getAutoCommit().getCmd());
//                    str.append(";");
//                    return false;
//                }
//                break;
//            case SLAVE_NODE:
//                break;
//        }
//        if (!mycat.getCharset().equals(this.getCharset())) {
//            str.append(mycat.getAutoCommit().getCmd());
//            str.append(";");
//            return false;
//        }
//        return targetDatabase == null || this.getTargetDatabase().equals(targetDatabase);
//    }


//    public void setTargetDatabase(String targetDatabase) {
//        this.targetDatabase = targetDatabase;
//    }
//
//    public void synchronizedState(MySQLIsolation isolation, MySQLAutoCommit autoCommit, MySQLCharset charset, AsynTaskCallBack<MySQLSession> finallyCallBack) throws IOException {
//        StringBuilder str = null;
//        MySQLMeta mysqlMeta = getMysqlMeta();
//        MycatSession mycat = getMycatSession();
//        boolean needSync = false;
//        switch (mysqlMeta.getType()) {
//            case MASTER_NODE:
//                if (!isolation.equals(this.getIsolation())) {
//                    if (str == null) {
//                        str = new StringBuilder();
//                    }
//                    str.append(mycat.getIsolation().getCmd());
//                    str.append(";");
//                    needSync = true;
//                }
//                if (!autoCommit.equals(this.getAutoCommit())) {
//                    if (str == null) {
//                        str = new StringBuilder();
//                    }
//                    str.append(mycat.getAutoCommit().getCmd());
//                    str.append(";");
//                    needSync = true;
//                }
//                break;
//            case SLAVE_NODE:
//                break;
//        }
//        if (!charset.equals(this.getCharset())) {
//            if (str == null) {
//                str = new StringBuilder();
//            }
//            str.append(mycat.getAutoCommit().getCmd());
//            str.append(";");
//            needSync = true;
//        }
//        boolean initDb = !(targetDatabase != null && !this.getTargetDatabase().equals(targetDatabase));
//        needSync = needSync || initDb;
//        if (needSync) {
//          //  new ResultSetTask(this, str.toString(),(MycatReactorThread)Thread.currentThread(),finallyCallBack);
//        } else {
//            finallyCallBack.finished(this, this, true, null, null);
//        }
//    }
}
