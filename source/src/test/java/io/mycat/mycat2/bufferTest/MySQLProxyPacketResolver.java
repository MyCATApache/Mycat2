package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

import static io.mycat.mycat2.bufferTest.MySQLRespPacketType.EOF;
import static io.mycat.mycat2.bufferTest.MySQLRespPacketType.UNKNOWN;

public class MySQLProxyPacketResolver {
    protected static Logger logger = LoggerFactory.getLogger(MySQLProxyPacketResolver.class);
    int sqlType = -1;
    int prepareFieldNum = 0;
    int prepareParamNum = 0;
    long columnCount = 0;
    int serverStatus = 0;
    int lastPacketId = 0;
    ComQueryRespState state = ComQueryRespState.FIRST_PACKET;
    boolean CLIENT_DEPRECATE_EOF;
    public MySQLRespPacketType mysqlPacketType = MySQLRespPacketType.UNKNOWN;
    boolean crossPacket = false;
    CapabilityFlags capabilityFlags;

    public MySQLProxyPacketResolver() {
        this(MySQLSession.getClientCapabilityFlags(), Boolean.FALSE);
    }

    public MySQLProxyPacketResolver(CapabilityFlags capabilityFlags, boolean CLIENT_DEPRECATE_EOF) {
        this.capabilityFlags = capabilityFlags;
        this.CLIENT_DEPRECATE_EOF = CLIENT_DEPRECATE_EOF;
    }

    public PayloadType resolveFullPayload(PacketInf packetInf) {
        PacketType type = resolveMySQLPackage(packetInf);
        if (type == PacketType.FULL) {//终止条件
            return !this.crossPacket ? PayloadType.FULL_PAYLOAD : PayloadType.LONG_PAYLOAD;
        } else {
            return type.fullPayloadType;
        }
    }

    public PayloadType resolveCrossBufferFullPayload(PacketInf packetInf) {
        boolean crossPacket = this.crossPacket;
        PacketType type = resolveMySQLPackage(packetInf);
        if (!this.crossPacket && (type == PacketType.FINISHED_CROSS || type == PacketType.FULL)) {
            return PayloadType.FINISHED_CROSS_PAYLOAD;
        } else if (type == PacketType.LONG_HALF) {
            return crossBuffer(packetInf) || crossPacket ? PayloadType.REST_CROSS_PAYLOAD : PayloadType.SHORT_PAYLOAD;
        } else if (type == PacketType.SHORT_HALF) {
            return PayloadType.SHORT_PAYLOAD;
        } else {
            return PayloadType.REST_CROSS_PAYLOAD;
        }
    }

    public PacketType resolveMySQLPackage(PacketInf packetInf) {
        int offset = packetInf.proxyBuffer.readIndex;   // 读取的偏移位置
        int limit = packetInf.proxyBuffer.writeIndex;   // 读取的总长度
        int totalLen = limit - offset;      // 读取当前的总长度
        ByteBuffer buffer = packetInf.proxyBuffer.getBuffer();
        switch (packetInf.packetType) {
            case SHORT_HALF:
            case FULL:
            case FINISHED_CROSS: {
                if (totalLen > 3) {//totalLen >= 4
                    int packetId = buffer.get(offset + 3) & 0xff;
                    checkPacketId(packetId);
                    int payloadLength = ParseUtil.getPayloadLength(buffer, offset);
                    boolean isCrossPacket = this.crossPacket;
                    this.crossPacket = payloadLength == 0xffffff;
                    if (this.crossPacket) {
                        packetInf.pkgLength = 0xffffff;
                    } else {
                        packetInf.pkgLength = payloadLength + 4;
                    }
                    boolean isPacketFinished = totalLen >= packetInf.pkgLength;
                    if (isCrossPacket) {
                        return resolveLongHalf(packetInf, offset, limit, totalLen);
                    } else {
                        if (totalLen > 4) {
                            return resolveShort2FullOrLongHalf(packetInf, offset, limit, buffer, isPacketFinished);
                        } else if (totalLen == 4 && packetInf.pkgLength == 4) {
                            this.mysqlPacketType = MySQLRespPacketType.UNKNOWN;
                            this.crossPacket = false;
                            packetInf.endPos = 4;
                            return packetInf.packetType = PacketType.FULL;
                        } else {
                            return resolveShortHalf(packetInf);
                        }
                    }
                } else {
                    return resolveShortHalf(packetInf);
                }
            }
            case LONG_HALF:
                return resolveLongHalf(packetInf, offset, limit, totalLen);
            case REST_CROSS:
                return resolveRestCross(packetInf, offset, limit, totalLen);
            default:
                throw new RuntimeException("unknown state!");

        }
    }

    private PacketType resolveShortHalf(PacketInf packetInf) {
        this.crossPacket = false;
        this.mysqlPacketType = UNKNOWN;
        return packetInf.change2ShortHalf();
    }

    private PacketType resolveShort2FullOrLongHalf(PacketInf packetInf, int offset, int limit, ByteBuffer buffer, boolean isPacketFinished) {
        packetInf.head = buffer.get(offset + 4) & 0xff;
        checkNeedFull(packetInf);
        packetInf.startPos = offset;
        if (isPacketFinished) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (this.state.needFull && !this.crossPacket) {
                resolvePayloadType(packetInf, true);
            }
            return packetInf.packetType = PacketType.FULL;
        } else {
            if (!this.state.needFull) {
                resolvePayloadType(packetInf, isPacketFinished);
            }
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
    }


    private PacketType resolveLongHalf(PacketInf packetInf, int offset, int limit, int totalLen) {
        packetInf.startPos = offset;
        if (totalLen >= packetInf.pkgLength) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (this.state.needFull && !this.crossPacket) {
                resolvePayloadType(packetInf, true);
            }
            return packetInf.packetType = PacketType.FULL;
        } else {
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
    }

    private PacketType resolveRestCross(PacketInf packetInf, int offset, int limit, int totalLen) {
        if (packetInf.remainsBytes <= totalLen) {// 剩余报文结束
            packetInf.endPos = offset + packetInf.remainsBytes;
            offset += packetInf.remainsBytes; // 继续处理下一个报文
            packetInf.proxyBuffer.readIndex = offset;
            packetInf.remainsBytes = 0;
            return packetInf.packetType = PacketType.FINISHED_CROSS;
        } else {// 剩余报文还没读完，等待下一次读取
            packetInf.startPos = 0;
            packetInf.remainsBytes -= totalLen;
            packetInf.endPos = limit;
            packetInf.proxyBuffer.readIndex = packetInf.endPos;
            return packetInf.packetType = PacketType.REST_CROSS;
        }
    }

    private void checkNeedFull(PacketInf packetInf) {
        if ((state == ComQueryRespState.RESULTSET_ROW) && (packetInf.head == 0xfe) && packetInf.pkgLength < 0xffffff) {
            this.state = ComQueryRespState.RESULTSET_ROW_END;
        }
    }

    private void checkPacketId(int packetId) {
        if (++lastPacketId != packetId) {
            throw new RuntimeException("packetId should be " + lastPacketId + " that is not match " + packetId);
        }
    }


    public void resolvePayloadType(PacketInf packetInf, boolean isPacketFinished) {
        int head = packetInf.head;
        switch (state) {
            case QUERY_PACKET:{
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                state = ComQueryRespState.END;
                return;
            }
            case FIRST_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 0xff) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    state = ComQueryRespState.END;
                } else if (head == 0x00) {
                    if (sqlType == MySQLCommand.COM_STMT_PREPARE && packetInf.pkgLength == 16
                        //&& packetInf.packetId == 1
                    ) {
                        resolvePrepareOkPacket(packetInf);
                        return;
                    } else {
                        this.mysqlPacketType = MySQLRespPacketType.OK;
                        this.serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                        state = ComQueryRespState.END;
                        return;
                    }
                } else if (head == 0xfb) {
                    throw new UnsupportedOperationException("unsupport LOCAL INFILE!");
                } else if (head == 0xfe) {
                    this.mysqlPacketType = EOF;
                    this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                    state = ComQueryRespState.END;
                } else {
                    ProxyBuffer proxyBuffer = packetInf.proxyBuffer;
                    columnCount = proxyBuffer.getLenencInt(packetInf.startPos + 4);
                    this.mysqlPacketType = MySQLRespPacketType.COLUMN_COUNT;
                    state = ComQueryRespState.COLUMN_DEFINITION;
                }
                return;
            }
            case COLUMN_DEFINITION: {
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryRespState.COLUMN_END_EOF : ComQueryRespState.RESULTSET_ROW;
                }
                this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
                return;
            }
            case COLUMN_END_EOF: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                this.mysqlPacketType = EOF;
                this.state = ComQueryRespState.RESULTSET_ROW;
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    //binary resultset row
                    this.mysqlPacketType = MySQLRespPacketType.BINARY_RESULTSET_ROW;
                } else if (head == 0xfe && packetInf.pkgLength < 0xffffff) {
                    if (!isPacketFinished) throw new RuntimeException("unknown state!");
                    if (CLIENT_DEPRECATE_EOF) {
                        this.mysqlPacketType = MySQLRespPacketType.OK;
                        //ok
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    } else {
                        this.mysqlPacketType = EOF;
                        //eof
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    }
                    if (JudgeUtil.hasMoreResult(serverStatus)) {
                        state = ComQueryRespState.FIRST_PACKET;
                    } else {
                        state = ComQueryRespState.END;
                    }
                } else if (head == MySQLPacket.ERROR_PACKET) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    state = ComQueryRespState.END;
                } else {
                    this.mysqlPacketType = MySQLRespPacketType.TEXT_RESULTSET_ROW;
                    //text resultset row
                }
                break;
            }
            default: {
                resolvePrepareResponse(packetInf.proxyBuffer, head, isPacketFinished);
            }
        }
    }

    private void resolvePrepareOkPacket(PacketInf packetInf) {
        ProxyBuffer buffer = packetInf.proxyBuffer;
        buffer.readIndex = packetInf.startPos + 9;
        this.prepareFieldNum = (int) buffer.readFixInt(2);
        this.prepareParamNum = (int) buffer.readFixInt(2);
        this.mysqlPacketType = MySQLRespPacketType.PREPARE_OK;
        if (this.prepareFieldNum == 0 && this.prepareParamNum == 0) {
            state = ComQueryRespState.END;
            return;
        } else if (this.prepareFieldNum > 0){
            state = ComQueryRespState.PREPARE_FIELD;
            return;
        }
        if(this.prepareParamNum > 0){
            state = ComQueryRespState.PREPARE_PARAM;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

    private void resolvePrepareResponse(ProxyBuffer proxyBuf, int head, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (prepareFieldNum > 0 && (state == ComQueryRespState.PREPARE_FIELD)) {
            prepareFieldNum--;
            this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
            this.state = ComQueryRespState.PREPARE_FIELD;
            if (prepareFieldNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    this.state = ComQueryRespState.PREPARE_FIELD_EOF;
                } else if (prepareParamNum>0){
                    this.state = ComQueryRespState.PREPARE_PARAM;
                }else {
                    this.state = ComQueryRespState.END;
                }
            }
            return;
        } else if (this.state == ComQueryRespState.PREPARE_FIELD_EOF && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            this.state = ComQueryRespState.PREPARE_PARAM;
            return;
        } else if (prepareParamNum > 0 && this.state == ComQueryRespState.PREPARE_PARAM) {
            prepareParamNum--;
            this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
            this.state = ComQueryRespState.PREPARE_PARAM;
            if (prepareParamNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    state = ComQueryRespState.PREPARE_PARAM_EOF;
                    return;
                } else {
                    state = ComQueryRespState.END;
                    return;
                }
            } else {
                return;
            }
        } else if (prepareFieldNum == 0 && prepareParamNum == 0 && !CLIENT_DEPRECATE_EOF && ComQueryRespState.PREPARE_PARAM_EOF == state && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            state = ComQueryRespState.END;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

    static enum ComQueryRespState {
        QUERY_PACKET(true),
        FIRST_PACKET(true),
        COLUMN_DEFINITION(false),
        COLUMN_END_EOF(true),
        RESULTSET_ROW(false),
        RESULTSET_ROW_END(true),
        PREPARE_FIELD(false),
        PREPARE_FIELD_EOF(true),
        PREPARE_PARAM(false),
        PREPARE_PARAM_EOF(true),
        END(false);
        boolean needFull;

        ComQueryRespState(boolean needFull) {
            this.needFull = needFull;
        }
    }

    public boolean crossBuffer(PacketInf packetInf) {
        if (packetInf.packetType == PacketType.LONG_HALF && !this.state.needFull) {
            packetInf.proxyBuffer.readIndex = packetInf.endPos;
            packetInf.updateState(packetInf.head, packetInf.startPos, packetInf.endPos, packetInf.pkgLength,
                    packetInf.pkgLength - (packetInf.endPos - packetInf.startPos), PacketType.REST_CROSS, packetInf.proxyBuffer);
            return true;
        } else {
            return false;
        }
    }

    public static class PacketInf {
        int head;
        int startPos;
        int endPos;
        int pkgLength;
        int remainsBytes;//还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
        PacketType packetType = PacketType.SHORT_HALF;
        //boolean needExpandBuffer;
        ProxyBuffer proxyBuffer;

        public void updateState(
                int head,
                int startPos,
                int endPos,
                int pkgLength,
                int remainsBytes,
                PacketType packetType,
                ProxyBuffer proxyBuffer
        ) {
            String oldState = null;
            if (logger.isDebugEnabled()) {
                oldState = this.toString();
                logger.debug("from {}", oldState);
            }
            this.head = head;
            this.startPos = startPos;
            this.endPos = endPos;
            this.pkgLength = pkgLength;
            this.remainsBytes = remainsBytes;
            this.packetType = packetType;
            // this.needExpandBuffer = needExpandBuffer;
            this.proxyBuffer = proxyBuffer;
            if (logger.isDebugEnabled()) {
                logger.debug("to   {}", this.toString());
            }
        }


        public PacketInf(ProxyBuffer proxyBuffer) {
            this.proxyBuffer = proxyBuffer;
        }

        public boolean needExpandBuffer() {
            return startPos == 0 && pkgLength > endPos && pkgLength > proxyBuffer.getBuffer().capacity();
        }

        public PacketType change2ShortHalf() {
            this.updateState(0, 0, 0, 0, 0, PacketType.SHORT_HALF, proxyBuffer);
            return PacketType.SHORT_HALF;
        }


        public void markRead() {
            if (packetType == PacketType.FULL || packetType == PacketType.REST_CROSS || packetType == PacketType.FINISHED_CROSS) {
                this.proxyBuffer.readIndex = endPos;
            } else {
                throw new UnsupportedOperationException("markRead is only in FULL or REST_CROSS or FINISHED_CROSS");
            }
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", PacketInf.class.getSimpleName() + "[", "]")
                    .add("head=" + head)
                    .add("startPos=" + startPos)
                    .add("endPos=" + endPos)
                    .add("pkgLength=" + pkgLength)
                    .add("remainsBytes=" + remainsBytes)
                    .add("packetType=" + packetType)
                    .add("proxyBuffer=" + proxyBuffer)
                    .toString();
        }
    }

    public static enum PacketType {
        FULL(PayloadType.UNKNOWN, PayloadType.FULL_PAYLOAD),
        LONG_HALF(PayloadType.UNKNOWN, PayloadType.LONG_PAYLOAD),
        SHORT_HALF(PayloadType.SHORT_PAYLOAD, PayloadType.SHORT_PAYLOAD),
        REST_CROSS(PayloadType.REST_CROSS_PAYLOAD, PayloadType.UNKNOWN),
        FINISHED_CROSS(PayloadType.FINISHED_CROSS_PAYLOAD, PayloadType.UNKNOWN);
        PayloadType corssPayloadType;
        PayloadType fullPayloadType;

        PacketType(PayloadType corssPayloadType, PayloadType fullPayloadType) {
            this.corssPayloadType = corssPayloadType;
            this.fullPayloadType = fullPayloadType;
        }
    }

    public static enum PayloadType {
        UNKNOWN, SHORT_PAYLOAD, LONG_PAYLOAD, FULL_PAYLOAD, REST_CROSS_PAYLOAD, FINISHED_CROSS_PAYLOAD
    }
}
