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
package io.mycat.proxy.handler;

import io.mycat.command.CommandResolver;
import io.mycat.command.ThreadModeCommandHanlderImpl;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.FrontMySQLPacketResolver;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.ProcessState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

/**
 * @author jamie12221
 * The enum Mycat handlerName.
 */
public enum MycatHandler implements NIOHandler<MycatSession> {
    /**
     * PhysicsInstanceImpl mycat handlerName.
     */
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatHandler.class);

    final
    @Override
    public void onSocketRead(MycatSession mycat) {
        try {
//      if (!mycat.checkOpen()) {
//        onException(mycat, new ClosedChannelException());
//        mycat.close(false, "mysql session has closed");
//        return;
//      }
            FrontMySQLPacketResolver resolver = mycat.getMySQLPacketResolver();
            ProcessState processState = mycat.getProcessState();
            if (processState == ProcessState.READY) {
                if (resolver.readFromChannel()) {
                    mycat.clearReadWriteOpts();

                        CommandResolver.handle(mycat, resolver.getPayload(),new ThreadModeCommandHanlderImpl( mycat.getCommandHandler()));

                    return;
                } else {
                    return;
                }
            }
        } catch (ClosedChannelException e) {
            MycatMonitor.onMycatHandlerCloseException(mycat, e);
            mycat.close(true,e);
            return;
        } catch (Exception e) {
            MycatMonitor.onMycatHandlerReadException(mycat, e);
            onException(mycat, e);
        }
    }

    @Override
    public void onSocketWrite(MycatSession mycat) {
        try {
            if ((mycat.getChannelKey().interestOps() & SelectionKey.OP_WRITE) != 0) {
                mycat.writeToChannel();
            }
        } catch (Exception e) {
            MycatMonitor.onMycatHandlerWriteException(mycat, e);
            mycat.close(false,e);
        }
    }

    @Override
    public void onWriteFinished(MycatSession mycat) {
        try {
            if (mycat.isResponseFinished()) {
                mycat.onHandlerFinishedClear();
            } else {
                mycat.writeToChannel();
            }
        } catch (Exception e) {
            MycatMonitor.onMycatHandlerWriteException(mycat, e);
            mycat.close(false,e);
        }
    }

    @Override
    public void onException(MycatSession mycat, Exception e) {
        MycatMonitor.onMycatHandlerException(mycat, e);
        LOGGER.error("", e);
        MycatSessionWriteHandler writeHandler = mycat.getWriteHandler();
        if (writeHandler != null) {
            writeHandler.onException(mycat, e);
        }
        onClear(mycat);
        mycat.writeErrorEndPacketBySyncInProcessError();
    }

    /**
     * On clear.
     *
     * @param session the session
     */
    public void onClear(MycatSession session) {
        session.resetPacket();
        MycatMonitor.onMycatHandlerClear(session);
    }


}
