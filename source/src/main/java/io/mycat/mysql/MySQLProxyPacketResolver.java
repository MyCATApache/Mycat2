package io.mycat.mysql;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static io.mycat.mysql.MySQLRespPacketType.EOF;
import static io.mycat.mysql.MySQLRespPacketType.UNKNOWN;

public class MySQLProxyPacketResolver {
    private final static Logger logger = LoggerFactory.getLogger(MySQLProxyPacketResolver.class);
    public int sqlType = -1;
    public int prepareFieldNum = 0;
    public int prepareParamNum = 0;
    public long columnCount = 0;
    public int serverStatus = 0;
    public byte lastPacketId = 0;
    public ComQueryState state = ComQueryState.FIRST_PACKET;
    public final boolean CLIENT_DEPRECATE_EOF;
    public MySQLRespPacketType mysqlPacketType = MySQLRespPacketType.UNKNOWN;
    public boolean crossPacket = false;
    public final CapabilityFlags capabilityFlags;

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
                    byte packetId = buffer.get(offset + 3);
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

    private void checkNeedFull(PacketInf packetInf) {
        if ((state == ComQueryState.RESULTSET_ROW) && (packetInf.head == 0xfe) && packetInf.pkgLength < 0xffffff) {
            this.state = ComQueryState.RESULTSET_ROW_END;
        }
    }

    private void checkPacketId(byte packetId) {
        if (++lastPacketId != packetId) {
            throw new RuntimeException("packetId should be " + lastPacketId + " that is not match " + packetId);
        }
    }


    public void resolvePayloadType(PacketInf packetInf, boolean isPacketFinished) {
        int head = packetInf.head;
        switch (state) {
            case QUERY_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                state = ComQueryState.END;
                return;
            }
            case FIRST_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 0xff) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    state = ComQueryState.END;
                } else if (head == 0x00) {
                    if (sqlType == MySQLCommand.COM_STMT_PREPARE && packetInf.pkgLength == 16
                        //&& packetInf.packetId == 1
                    ) {
                        resolvePrepareOkPacket(packetInf, isPacketFinished);
                        return;
                    } else {
                        this.mysqlPacketType = MySQLRespPacketType.OK;
                        this.serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                        state = ComQueryState.END;
                        return;
                    }
                } else if (head == 0xfb) {
                    throw new UnsupportedOperationException("unsupport LOCAL INFILE!");
                } else if (head == 0xfe) {
                    this.mysqlPacketType = EOF;
                    this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                    state = ComQueryState.END;
                } else {
                    ProxyBuffer proxyBuffer = packetInf.proxyBuffer;
                    columnCount = proxyBuffer.getLenencInt(packetInf.startPos + 4);
                    this.mysqlPacketType = MySQLRespPacketType.COLUMN_COUNT;
                    state = ComQueryState.COLUMN_DEFINITION;
                }
                return;
            }
            case COLUMN_DEFINITION: {
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryState.COLUMN_END_EOF : ComQueryState.RESULTSET_ROW;
                }
                this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
                return;
            }
            case COLUMN_END_EOF: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                this.mysqlPacketType = EOF;
                this.state = ComQueryState.RESULTSET_ROW;
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    //binary resultset row
                    this.mysqlPacketType = MySQLRespPacketType.BINARY_RESULTSET_ROW;
                } else if (head == 0xfe && packetInf.pkgLength < 0xffffff) {
                    resolveResultsetRowEnd(packetInf, isPacketFinished);
                } else if (head == MySQLPacket.ERROR_PACKET) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    state = ComQueryState.END;
                } else {
                    this.mysqlPacketType = MySQLRespPacketType.TEXT_RESULTSET_ROW;
                    //text resultset row
                }
                break;
            }
            case RESULTSET_ROW_END:
                resolveResultsetRowEnd(packetInf, isPacketFinished);
                break;
            case PREPARE_FIELD:
            case PREPARE_FIELD_EOF:
            case PREPARE_PARAM:
            case PREPARE_PARAM_EOF:
                resolvePrepareResponse(packetInf.proxyBuffer, head, isPacketFinished);
                return;
            case END:
            default: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
            }
        }
    }

    private void resolveResultsetRowEnd(PacketInf packetInf, boolean isPacketFinished) {
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
            state = ComQueryState.FIRST_PACKET;
        } else {
            state = ComQueryState.END;
        }
    }

    private void resolvePrepareOkPacket(PacketInf packetInf, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        ProxyBuffer buffer = packetInf.proxyBuffer;
        buffer.readIndex = packetInf.startPos + 9;
        this.prepareFieldNum = (int) buffer.readFixInt(2);
        this.prepareParamNum = (int) buffer.readFixInt(2);
        this.mysqlPacketType = MySQLRespPacketType.PREPARE_OK;
        if (this.prepareFieldNum == 0 && this.prepareParamNum == 0) {
            state = ComQueryState.END;
            return;
        } else if (this.prepareFieldNum > 0) {
            state = ComQueryState.PREPARE_FIELD;
            return;
        }
        if (this.prepareParamNum > 0) {
            state = ComQueryState.PREPARE_PARAM;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

    private void resolvePrepareResponse(ProxyBuffer proxyBuf, int head, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (prepareFieldNum > 0 && (state == ComQueryState.PREPARE_FIELD)) {
            prepareFieldNum--;
            this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
            this.state = ComQueryState.PREPARE_FIELD;
            if (prepareFieldNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    this.state = ComQueryState.PREPARE_FIELD_EOF;
                } else if (prepareParamNum > 0) {
                    this.state = ComQueryState.PREPARE_PARAM;
                } else {
                    this.state = ComQueryState.END;
                }
            }
            return;
        } else if (this.state == ComQueryState.PREPARE_FIELD_EOF && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            this.state = ComQueryState.PREPARE_PARAM;
            return;
        } else if (prepareParamNum > 0 && this.state == ComQueryState.PREPARE_PARAM) {
            prepareParamNum--;
            this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
            this.state = ComQueryState.PREPARE_PARAM;
            if (prepareParamNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    state = ComQueryState.PREPARE_PARAM_EOF;
                    return;
                } else {
                    state = ComQueryState.END;
                    return;
                }
            } else {
                return;
            }
        } else if (prepareFieldNum == 0 && prepareParamNum == 0 && !CLIENT_DEPRECATE_EOF && ComQueryState.PREPARE_PARAM_EOF == state && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            state = ComQueryState.END;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

}
