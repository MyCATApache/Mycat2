/**
 * Copyright (C) <2022>  <chen junwen>
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

package io.mycat.mysqlclient.command;

import io.mycat.mysqlclient.Command;
import io.mycat.mysqlclient.PacketUtil;
import io.mycat.newquery.SqlResult;
import io.mycat.vertx.VertxMySQLPacketClientResolver;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.mysqlclient.impl.protocol.Packets;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vertx.mysqlclient.impl.protocol.Packets.ERROR_PACKET_HEADER;

@Getter
public class OkCommand extends VertxMySQLPacketClientResolver implements Command {
    final static Logger logger = LoggerFactory.getLogger(OkCommand.class);
    final String text;
    private Promise<SqlResult> promise;
    public int serverstatus;

    public OkCommand(String text, NetSocket socket, Promise<SqlResult> promise) {
        super(socket, 0);
        this.text = text;
        this.promise = promise;
    }

    @Override
    public void write() {
        if (logger.isDebugEnabled()){
            logger.debug("send sql:{}",text);
        }
        packetId = PacketUtil.writeQueryText(socket, text);
    }

    @Override
    public void handle0(int packetId, Buffer payload, NetSocket socket) {
        try{
            short firstByte = payload.getUnsignedByte(0);
            switch (firstByte) {
                case ERROR_PACKET_HEADER: {
                    promise.fail(PacketUtil.handleErrorPacketPayload(payload));
                    break;
                }
                default: {
                    Packets.OkPacket okPacket = PacketUtil.decodeOkPacketPayload(payload);
                    serverstatus = okPacket.serverStatusFlags();
                    promise.tryComplete(SqlResult.of(okPacket.affectedRows(), okPacket.lastInsertId()));
                    break;
                }
            }
            return;
        }catch (Exception e){
            promise.fail(e);
        }

    }

    @Override
    public void onException(Throwable throwable) {
        promise.fail(throwable);
    }
}
