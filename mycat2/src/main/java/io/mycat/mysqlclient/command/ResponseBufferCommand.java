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

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.mysqlclient.Command;
import io.mycat.mysqlclient.MetaPacket;
import io.mycat.mysqlclient.PacketUtil;
import io.mycat.mysqlclient.VertxPoolConnectionImpl;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.netty.buffer.ByteBuf;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.mysqlclient.impl.util.BufferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.proxy.packet.MySQLPacketResolver.ComQueryState.RESULTSET_ROW;
import static io.mycat.proxy.packet.MySQLPayloadType.*;

public class ResponseBufferCommand implements Handler<Buffer>, Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseBufferCommand.class);

    private NetSocket socket;
    private String text;
    private ObservableEmitter<Buffer> emitter;


    public ResponseBufferCommand(NetSocket socket, String text, VertxPoolConnectionImpl.Config config, boolean fullPacket, ObservableEmitter<Buffer> emitter) {
        this.socket = socket;
        this.text = text;
        this.config = config;
        this.fullPacket = fullPacket;
        this.emitter = emitter;
    }

    @Override
    public void write() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send sql:{}", text);
        }
        parsePacketId = PacketUtil.writeQueryText(this.socket, text);
    }

    private VertxPoolConnectionImpl.Config config;
    private final boolean fullPacket;
    byte parsePacketId;
    int payloadLength;
    int remainsBytes;
    boolean multiPacket;
    byte packetId = 0;
    int head;
    int startPos;
    int endPos;
    boolean payloadFinished;

    MySQLPayloadType payloadType;
    Buffer curBuffer;
    MySQLPacketResolver.ComQueryState state = MySQLPacketResolver.ComQueryState.FIRST_PACKET;

    MetaPacket metaPacket = new MetaPacket();


    @Override
    public void handle(Buffer add) {
        if (this.curBuffer == null) {
            this.curBuffer = add;
        } else {
            this.curBuffer.appendBuffer(add);
        }
        for (; ; ) {
            boolean next;
            if (fullPacket || state.isNeedFull()) {
                payloadType = null;
                next = readFullPacket();
                if (payloadType != null) {
                    switch (payloadType) {
                        case FIRST_ERROR:
                        case ROW_ERROR:
                            onEnd();
                            emitter.onError(PacketUtil.handleErrorPacketPayload(getMetaPayload()));
                            break;
                        default:
                            break;
                        case COLUMN_COUNT:
                            decodeColumnCountPacketPayload(getMetaPayload().getByteBuf());
                            break;
                    }

                }
            } else {
                next = readHalfFull();
            }

            if (!next) {
                break;
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("payloadLength:{}", (payloadLength));
                    LOGGER.debug("packetLength:{}", (endPos - startPos));
                    LOGGER.debug("remainsBytes:{}", (remainsBytes));
                    LOGGER.debug("-------------------------------------------");
                }

                this.startPos = this.endPos;
            }
        }
        if (this.endPos == this.curBuffer.length()) {
            Buffer curBuffer = this.curBuffer;
            this.curBuffer = null;
            this.startPos = 0;
            this.endPos = 0;
            emitter.onNext(curBuffer);
        } else {
            Buffer slice = this.curBuffer.slice(0, this.endPos);
            Buffer newBuffer = Buffer.buffer(2*this.curBuffer.length());
            this.curBuffer = newBuffer.appendBuffer(this.curBuffer, this.endPos, this.curBuffer.length()-this.endPos);
            this.startPos = 0;
            this.endPos = 0;
            emitter.onNext(slice);
        }
        if (state == MySQLPacketResolver.ComQueryState.COMMAND_END) {
            onEnd();
            emitter.onComplete();
        }
    }

    public void onEnd() {

    }

    private void log(String s) {

    }

    private Buffer getMetaPayload() {
        int payloadStartIndex = this.startPos + 4;
        int payloadEndIndex = this.endPos;
        return this.curBuffer.getBuffer(payloadStartIndex, payloadEndIndex);
    }

    private void decodeColumnCountPacketPayload(ByteBuf payload) {
        long columnCount = BufferUtils.readLengthEncodedInteger(payload);
        metaPacket.columnCount = (int) columnCount;
    }

    private boolean readHalfFull() {
        int startIndex = this.startPos;
        int receiveSize = this.curBuffer.length() - startIndex;
        if (receiveSize < 0) {
            throw new IllegalArgumentException("receiveSize : " + receiveSize);
        }
        if (receiveSize == 0) {
            return false;
        }
        if (this.remainsBytes <= 0) {
            if (receiveSize < 4) {
                return false;
            }
            int packetLength = (int) readFixInt(this.curBuffer, startIndex, 3);
            this.parsePacketId = (byte) (this.curBuffer.getByte(startIndex + 3) & 0xff);

            this.multiPacket = (packetLength == MySQLPacketSplitter.MAX_PACKET_SIZE);
            if (packetLength == 0) {
                this.checkPacketId();
                this.startPos = startIndex;
                this.endPos = (startIndex + 4);
                this.remainsBytes = 0;
                return true;
            }
            if (receiveSize < 5) {
                return false;
            }
            this.checkPacketId();
            int aByte = curBuffer.getByte(startIndex + 4) & 0xff;
            this.head = (aByte);
            this.payloadLength = packetLength;
            if (state == RESULTSET_ROW && !multiPacket && (aByte
                    == 0xfe) && packetLength < 0xFFFFFF) {//for row end
                return readFullPacket();
            }

            if ((packetLength + 4) <= receiveSize) {
                startPos = (startIndex);
                endPos = (startIndex + packetLength + 4);
                remainsBytes = 0;
                // return true;
            } else {
                startPos = (startIndex);
                endPos = (startIndex) + receiveSize;
                remainsBytes = packetLength + 4 - receiveSize;
                return true;
            }
        } else {
            if (receiveSize >= remainsBytes) {
                startPos = (startIndex);
                endPos = (startIndex + remainsBytes);
                remainsBytes = 0;
            } else {
                remainsBytes -= receiveSize;
                startPos = (startIndex);
                endPos = (startIndex + receiveSize);
            }
        }
        payloadFinished = !multiPacket && remainsBytes == 0;
        if (payloadFinished) {
            resolvePayloadType(head, true, payloadLength);
        }
        boolean isPacketEnd = remainsBytes == 0;
        return true;
    }

    public void checkPacketId() {
        if ((this.parsePacketId - this.packetId) > 1) {
            throw new IllegalArgumentException("packet error " + this.packetId + " to " + parsePacketId);
        }
        this.packetId = parsePacketId;
        this.parsePacketId = parsePacketId;
    }

    public long readFixInt(Buffer buffer, int startIndex, int length) {
        int rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = buffer.getByte(startIndex + i);
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }

    private int setColumnCount(int i) {
        return metaPacket.columnCount = i;
    }

    private int getColumnCount() {
        return metaPacket.columnCount;
    }

    private boolean readFullPacket() {
        int reveiceSize = this.curBuffer.length() - this.startPos;
        if (reveiceSize == 0) {
            return false;
        }
        if (reveiceSize < 0) {
            throw new IllegalArgumentException("reveiceSize < 0 ");
        }
        if (this.remainsBytes == 0) {
            if (reveiceSize < 4) {
                return false;
            }
            int packetLength = (int) readFixInt(this.curBuffer, this.startPos, 3);
            this.parsePacketId = (byte) (this.curBuffer.getByte(this.startPos + 3) & 0xff);
            this.checkPacketId();
            this.payloadLength = packetLength;
            this.multiPacket = packetLength == MySQLPacketSplitter.MAX_PACKET_SIZE;
            if ((packetLength + 4) <= reveiceSize) {
                this.remainsBytes = 0;
                this.startPos = this.startPos;
                this.endPos = this.startPos + packetLength + 4;
                int aByte = curBuffer.getByte(this.startPos + 4) & 0xff;
                resolvePayloadType(aByte, true, payloadLength);
                return true;
            }
        }
        return false;
    }


    void resolvePayloadType(int head, boolean isPacketFinished, int payloadLength) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("state:{}", state);
        }
        switch (state) {
            case QUERY_PACKET: {
                if (!isPacketFinished) {
                    throw new RuntimeException("unknown state!");
                }
                switch (head) {
                    case 3: {
                        state = (MySQLPacketResolver.ComQueryState.FIRST_PACKET);
                        payloadType = (REQUEST_COM_QUERY);
                        return;
                    }
                    case 24: {
                        state = (MySQLPacketResolver.ComQueryState.QUERY_PACKET);
                        payloadType = (REQUEST_SEND_LONG_DATA);
                        return;
                    }
                    case 25: {
                        state = (MySQLPacketResolver.ComQueryState.QUERY_PACKET);
                        payloadType = (REQUEST_COM_STMT_CLOSE);
                        return;
                    }
                    case 22: {
                        state = (MySQLPacketResolver.ComQueryState.FIRST_PACKET);
                        payloadType = (REQUEST_PREPARE);
                        return;
                    }
                    default: {
                        throw new IllegalArgumentException("unknown packet head:" + head);
                    }
                }
            }
            case AUTH_SWITCH_PLUGIN_RESPONSE:
            case AUTH_SWITCH_OTHER_REQUEST:
            case FIRST_PACKET: {
                if (!isPacketFinished) {
                    throw new MycatException("unknown state!");
                }
                if (head == 0xff) {
                    state = (MySQLPacketResolver.ComQueryState.COMMAND_END);
                    payloadType = (FIRST_ERROR);
                } else if (head == 0x00) {
                    throw new UnsupportedOperationException();
                } else if (head == 0xfb) {
                    throw new UnsupportedOperationException();
                } else if (head == 0xfe) {
//                            setServerStatus(eofPacketReadStatus(mySQLPacket));
                    state = (MySQLPacketResolver.ComQueryState.COMMAND_END);
                    payloadType = (FIRST_EOF);
                    return;
                } else {
                    state = (MySQLPacketResolver.ComQueryState.COLUMN_DEFINITION);
                    payloadType = (COLUMN_COUNT);
                }
                return;
            }
            case COLUMN_DEFINITION: {
                if (setColumnCount(getColumnCount() - 1) == 0) {
                    state = config.isClientDeprecateEof() ? RESULTSET_ROW : MySQLPacketResolver.ComQueryState.COLUMN_END_EOF;
                }
                payloadType = (COLUMN_DEF);
                return;
            }
            case COLUMN_END_EOF: {
                if (!isPacketFinished) {
                    throw new RuntimeException("unknown state!");
                }
                state = (RESULTSET_ROW);
                payloadType = (COLUMN_EOF);
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    throw new UnsupportedOperationException();
                } else if (head == 0xfe && payloadLength < 0xffffff) {
                    resolveResultsetRowEnd(isPacketFinished);
                    state = (MySQLPacketResolver.ComQueryState.COMMAND_END);//COMMAND_END结束完毕就切换到读状态
                } else if (head == 0xff) {
                    state = (MySQLPacketResolver.ComQueryState.RESULTSET_ROW_ERROR);
                    payloadType = (ROW_ERROR);
                    payloadType = null;//还需要切换状态到COMMAND_END
                } else {
                    //text resultset row
                    payloadType = (TEXT_ROW);
                }
                break;
            }
            case RESULTSET_ROW_ERROR: {
                state = (MySQLPacketResolver.ComQueryState.COMMAND_END);//COMMAND_END结束完毕就切换到读状态
                payloadType = (ROW_ERROR);
                break;
            }
            case PREPARE_FIELD:
                break;
            case PREPARE_FIELD_EOF:
                break;
            case PREPARE_PARAM:
                break;
            case PREPARE_PARAM_EOF:
                break;
            case COMMAND_END: {
            }
            return;
            case LOCAL_INFILE_OK_PACKET:
                break;
            default: {
                if (!isPacketFinished) {
                    throw new RuntimeException("unknown state!");
                } else {
                    throw new RuntimeException("unknown state!");
                }
            }
        }
    }

    void resolveResultsetRowEnd(boolean isPacketFinished) {
        if (!isPacketFinished) {
            throw new RuntimeException("unknown state!");
        }
        payloadType = (ROW_OK);
    }

}
