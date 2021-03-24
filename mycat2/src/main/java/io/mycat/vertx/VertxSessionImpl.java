/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.vertx;

import io.mycat.MySQLPacketUtil;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.session.ProcessState;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.NetSocket;

import java.nio.charset.Charset;

public class VertxSessionImpl implements VertxSession {
    private MycatDataContext mycatDataContext;
    private NetSocket socket;
    int packetId = 0;
    private ProcessState processState;

    public VertxSessionImpl(MycatDataContext mycatDataContext, NetSocket socket) {
        this.mycatDataContext = mycatDataContext;

        this.socket = socket;
    }

    @Override
    public void setPacketId(int packet) {
        this.packetId = packet;
    }

    @Override
    public byte getNextPacketId() {
        return (byte) (++packetId);
    }

    @Override
    public String getLastMessage() {
        return mycatDataContext.getLastMessage();
    }

    @Override
    public long affectedRows() {
        return mycatDataContext.getAffectedRows();
    }

    @Override
    public int getServerStatusValue() {
        return mycatDataContext.serverStatus();
    }

    @Override
    public int getWarningCount() {
        return mycatDataContext.getWarningCount();
    }

    @Override
    public boolean isDeprecateEOF() {
        return MySQLServerCapabilityFlags.isDeprecateEOF(this.mycatDataContext.getServerCapabilities());
    }

    @Override
    public long getLastInsertId() {
        return mycatDataContext.getLastInsertId();
    }

    @Override
    public int getLastErrorCode() {
        return mycatDataContext.getLastErrorCode();
    }

    @Override
    public void setLastErrorCode(int errorCode) {
        mycatDataContext.setLastErrorCode(errorCode);
    }

    @Override
    public int getCapabilities() {
        return this.mycatDataContext.getServerCapabilities();
    }

    @Override
    public boolean isResponseFinished() {
        return processState == ProcessState.DONE;
    }

    @Override
    public int charsetIndex() {
        return this.mycatDataContext.getCharsetIndex();
    }

    @Override
    public void setResponseFinished(ProcessState b) {
        this.processState = b;
    }

    @Override
    public void resetSession() {

    }

    @Override
    public Charset charset() {
        return this.mycatDataContext.getCharset();
    }

    @Override
    public PromiseInternal<Void> writeBytes(byte[] payload, boolean end) {
        if (end) {
            if (mycatDataContext!=null){
                TransactionSession transactionSession = mycatDataContext.getTransactionSession();
                if (transactionSession!=null){
                    transactionSession.closeStatementState();
                }
            }
        }
        Future<Void> future = socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(getNextPacketId(), payload)));
        if (end) {
            this.socket.resume();
        }
        return VertxUtil.castPromise(future);
    }

    @Override
    public PromiseInternal<Void> writeErrorEndPacketBySyncInProcessError() {
        return writeBytes(MySQLPacketUtil.generateError(
                mycatDataContext.getLastErrorCode(),
                mycatDataContext.getLastMessage(),
                getCapabilities()
        ), true);
    }

    @Override
    public PromiseInternal<Void> writeErrorEndPacketBySyncInProcessError(int errorCode) {
        return writeBytes(MySQLPacketUtil.generateError(
                errorCode,
                mycatDataContext.getLastMessage(),
                getCapabilities()
        ), true);
    }

    @Override
    public MycatDataContext getDataContext() {
        return mycatDataContext;
    }

    @Override
    public Future<Void> close(boolean b, String quit) {
        mycatDataContext.close();
        return socket.close();
    }

    @Override
    public PromiseInternal<Void>  close() {
        mycatDataContext.close();
        Future<Void> future = socket.close();
        return VertxUtil.castPromise(future);
    }

    @Override
    public NetSocket getSocket() {
        return socket;
    }
}
