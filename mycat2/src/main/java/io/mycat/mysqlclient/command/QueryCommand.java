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

import io.mycat.mysqlclient.*;
import io.mycat.vertx.VertxMySQLPacketClientResolver;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.mysqlclient.impl.util.BufferUtils;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vertx.mysqlclient.impl.protocol.Packets.EOF_PACKET_HEADER;
import static io.vertx.mysqlclient.impl.protocol.Packets.ERROR_PACKET_HEADER;

@Getter
public class QueryCommand<T> extends VertxMySQLPacketClientResolver implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCommand.class);
    private String text;
    private VertxPoolConnectionImpl.Config config;
    private Decoder<T> decoder;
    private ObservableEmitter<T> emitter;
    private boolean close;
    private int serverstatus;

    public QueryCommand(NetSocket socket, String text, VertxPoolConnectionImpl.Config config, Decoder<T> decoder, ObservableEmitter<T> emitter) {
        super(socket, 0);
        this.text = text;
        this.config = config;
        this.decoder = decoder;
        this.emitter = emitter;
    }

    @Override
    public void write() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("-------------------{}:queryCommand{}:netsocket{}:{}--------------------", Thread.currentThread(), this.hashCode(), this.socket.hashCode(), text);
        }
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("send sql:{}",text);
        }
        packetId = PacketUtil.writeQueryText(this.socket, text);
        close = false;
    }

    int columnCount;
    int columnCounter = 0;
    CommandHandlerState state = CommandHandlerState.INIT;


    void handleErrorPacketPayload(Buffer payload) {
        onEnd();
        emitter.onError(decoder.convertException(payload));
    }

    @Override
    public void handle0(int packetId, Buffer payload, NetSocket socket) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("-------------------queryCommand{}:socket{}:packetId:{}payload:{}--------------------", this.hashCode(), this.socket.hashCode(), packetId, payload);
        }
        try {
            if (close) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("-------close------------queryCommand{}:socket{}:packetId:{}payload:{}--------------------", this.hashCode(), this.socket.hashCode(), packetId, payload);
                }
            }
            switch (state) {
                case INIT: {
                    int firstByte = payload.getByte(0) & 0xff;
                    if (firstByte == 0xff) {
                        handleErrorPacketPayload(payload);
                        return;
                    }
                    columnCount = (int) BufferUtils.readLengthEncodedInteger(payload.getByteBuf());
                    decoder.initColumnCount(columnCount);
                    columnCounter = 0;
                    state = CommandHandlerState.HANDLING_COLUMN_DEFINITION;
                    break;
                }
                case HANDLING_COLUMN_DEFINITION:
                    decoder.addColumn(columnCounter, payload);
                    columnCounter++;
                    if (columnCounter == columnCount) {
                        state = config.isClientDeprecateEof() ?
                                CommandHandlerState.HANDLING_ROW_DATA_OR_END_PACKET :
                                CommandHandlerState.COLUMN_DEFINITIONS_DECODING_COMPLETED;
                        decoder.onColumnEnd();
                    }
                    break;
                case COLUMN_DEFINITIONS_DECODING_COMPLETED: {
                    state = CommandHandlerState.HANDLING_ROW_DATA_OR_END_PACKET;
                    break;
                }
                case HANDLING_ROW_DATA_OR_END_PACKET: {
                    int first = payload.getUnsignedByte(0);
                    if (first == ERROR_PACKET_HEADER) {
                        handleErrorPacketPayload(payload);
                    } else if (first == EOF_PACKET_HEADER && payload.length() < 0xFFFFFF) {
                        serverstatus = PacketUtil.decodeOkPacketPayload(payload).serverStatusFlags();
                        close = true;
                        onEnd();
                        emitter.onComplete();
                    } else {
                        emitter.onNext(decoder.convert(payload));
                    }
                }
                break;
                default:
                    throw new IllegalStateException("Unexpected value: " + state);
            }
        } catch (Exception e) {
            emitter.onError(e);
        }
    }

    @Override
    public void onException(Throwable throwable) {
        emitter.onError(throwable);
    }

    public void onEnd() {

    }

}
