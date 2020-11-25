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

import io.mycat.Authenticator;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.buffer.BufferPool;
import io.mycat.command.CommandDispatcher;
import io.mycat.proxy.handler.front.MySQLClientAuthHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;

/**
 * 集中管理MySQL LocalInFileSession 是在mycat proxy中,唯一能够创建mysql session以及关闭mysqlsession的对象
 * 该在一个线程单位里,对象生命周期应该是单例的
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class MycatSessionManager implements FrontSessionManager<MycatSession> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MycatSessionManager.class);
    private final ConcurrentLinkedDeque<MycatSession> mycatSessions = new ConcurrentLinkedDeque<>();
    private final Function<MycatSession, CommandDispatcher> commandDispatcher;
    private final Authenticator authenticator;
    private final Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap;
    private final MycatContextThreadPool mycatContextThreadPool;

    public MycatSessionManager(Function<MycatSession, CommandDispatcher> function,
                               Authenticator authenticator,
                               Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap,
                               MycatContextThreadPool mycatContextThreadPool) {
        this.commandDispatcher = function;
        this.authenticator = Objects.requireNonNull(authenticator);
        this.transcationFactoryMap = transcationFactoryMap;
        this.mycatContextThreadPool = mycatContextThreadPool;
    }


    @Override

    public List<MycatSession> getAllSessions() {
        return new ArrayList<>(mycatSessions);
    }

    @Override
    public int currentSessionCount() {
        return mycatSessions.size();
    }

    /**
     * 调用该方法的时候 mycat session已经关闭了
     */
    @Override
    public void removeSession(MycatSession mycat, boolean normal, String reason) {
        try {
            MycatMonitor.onCloseMycatSession(mycat, normal, reason);
            mycatSessions.remove(mycat);
            mycat.channel().close();
        } catch (Exception e) {
            LOGGER.error("{}", e);
        }
    }


    @Override
    public void acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool,
                                       Selector nioSelector, SocketChannel frontChannel) throws IOException {
        MySQLClientAuthHandler mySQLClientAuthHandler = new MySQLClientAuthHandler(this);
        MycatSession mycat = new MycatSession(SessionManager.nextSessionId(), bufPool,
                mySQLClientAuthHandler, this, transcationFactoryMap, mycatContextThreadPool);


        //用于monitor监控获取session
        SessionThread thread = (SessionThread) Thread.currentThread();
        thread.setCurSession(mycat);
        try {
            mycat.register(nioSelector, frontChannel, SelectionKey.OP_READ);
            MycatMonitor.onNewMycatSession(mycat);
            mySQLClientAuthHandler.sendAuthPackge(mycat);
            this.mycatSessions.add(mycat);
        } catch (Exception e) {
            MycatMonitor.onAuthHandlerWriteException(mycat, e);
            mycat.close(false, e);
        }
    }

    @Override
    public void check() {
        Iterator<MycatSession> iterator = mycatSessions.iterator();
        while (iterator.hasNext()) {
            MycatSession next = iterator.next();
            if (next == null) {
                iterator.remove();
            } else {
                try {
                    if (!next.checkOpen()) {
                        next.close(false, "MycatSessionManager check");
                        LOGGER.warn("未关闭连接:{}", next);
                    }
                } catch (Exception e) {
                    LOGGER.error("{}", e);
                }
            }
        }
    }


    public void initCommandDispatcher(MycatSession session) {
        session.setCommandHandler(commandDispatcher.apply(session));
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }
}
