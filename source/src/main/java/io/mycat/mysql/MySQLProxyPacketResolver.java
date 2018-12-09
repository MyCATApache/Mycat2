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

import static io.mycat.mysql.MySQLPayloadType.*;

public class MySQLProxyPacketResolver {
    private final static Logger logger = LoggerFactory.getLogger(MySQLProxyPacketResolver.class);
    public int sqlType = -1;
    public int prepareFieldNum = 0;
    public int prepareParamNum = 0;
    public long columnCount = 0;
    public int serverStatus = 0;
    public byte nextPacketId = 0;
    public ComQueryState state = ComQueryState.FIRST_PACKET;
    public final boolean CLIENT_DEPRECATE_EOF;
    public MySQLPayloadType mysqlPacketType = MySQLPayloadType.UNKNOWN;
    public boolean crossPacket = false;
    public final CapabilityFlags capabilityFlags;

    public MySQLProxyPacketResolver() {
        this(MySQLSession.getClientCapabilityFlags(), Boolean.FALSE);
    }

    public MySQLProxyPacketResolver(CapabilityFlags capabilityFlags, boolean CLIENT_DEPRECATE_EOF) {
        this.capabilityFlags = capabilityFlags;
        this.CLIENT_DEPRECATE_EOF = CLIENT_DEPRECATE_EOF;
    }

    public void shift2DefRespPacket() {
        this.state = ComQueryState.RESP_END;
        this.serverStatus = 0;
    }
    public void shift2DefQueryPacket() {
        this.state = ComQueryState.FIRST_PACKET;
        this.serverStatus = 0;
        this.nextPacketId = 0;
    }
    public void shift2DoNot() {
        this.state = ComQueryState.DO_NOT;
    }

    public void shift2QueryPacket() {
        this.state = ComQueryState.QUERY_PACKET;
        this.nextPacketId = 0;
    }

    public void shift2RespPacket() {
        this.state = ComQueryState.FIRST_PACKET;
        this.nextPacketId = 1;
    }

    public PayloadType resolveFullPayload(MySQLPacketInf packetInf) {
        return resolveFullPayload(packetInf, packetInf.proxyBuffer);
    }

    public PayloadType resolveFullPayload(MySQLPacketInf packetInf, ProxyBuffer proxyBuffer) {
        PacketType type = resolveMySQLPacket(packetInf, proxyBuffer);
        if (type == PacketType.FULL) {//终止条件
            return !this.crossPacket ? PayloadType.FULL_PAYLOAD : PayloadType.LONG_PAYLOAD;
        } else {
            return type.fullPayloadType;
        }
    }

    public PayloadType resolveCrossBufferFullPayload(MySQLPacketInf packetInf) {
        return resolveCrossBufferFullPayload(packetInf, packetInf.proxyBuffer);
    }

    public PayloadType resolveCrossBufferFullPayload(MySQLPacketInf packetInf, ProxyBuffer proxyBuffer) {
        boolean crossPacket = this.crossPacket;
        PacketType type = resolveMySQLPacket(packetInf, proxyBuffer);
        if (!this.crossPacket && (type == PacketType.FINISHED_CROSS || type == PacketType.FULL)) {
            packetInf.markRead();
            return PayloadType.FINISHED_CROSS_PAYLOAD;
        } else if (type == PacketType.LONG_HALF) {
            PayloadType payloadType = crossBuffer(packetInf) || crossPacket ? PayloadType.REST_CROSS_PAYLOAD : PayloadType.SHORT_PAYLOAD;
            if (packetInf.packetType == PacketType.REST_CROSS){
                packetInf.markRead();
            }
            return payloadType;
        } else if (type == PacketType.SHORT_HALF) {
            return PayloadType.SHORT_PAYLOAD;
        } else {
            packetInf.markRead();
            return PayloadType.REST_CROSS_PAYLOAD;
        }
    }

    public PacketType resolveMySQLPacket(MySQLPacketInf packetInf) {
        return resolveMySQLPacket(packetInf, packetInf.proxyBuffer);
    }

    public PacketType resolveMySQLPacket(MySQLPacketInf packetInf, ProxyBuffer proxyBuffer) {
        packetInf.proxyBuffer = proxyBuffer;
        int offset = proxyBuffer.readIndex;   // 读取的偏移位置
        int limit = proxyBuffer.writeIndex;   // 读取的总长度
        int totalLen = limit - offset;      // 读取当前的总长度
        ByteBuffer buffer = proxyBuffer.getBuffer();
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
                            this.mysqlPacketType = MySQLPayloadType.UNKNOWN;
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

    private PacketType resolveShortHalf(MySQLPacketInf packetInf) {
        this.crossPacket = false;
        this.mysqlPacketType = UNKNOWN;
        return packetInf.change2ShortHalf();
    }

    private PacketType resolveShort2FullOrLongHalf(MySQLPacketInf packetInf, int offset, int limit, ByteBuffer buffer, boolean isPacketFinished) {
        packetInf.head = buffer.get(offset + 4) & 0xff;
        checkNeedFull(packetInf);
        packetInf.startPos = offset;
        if (isPacketFinished) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (!this.crossPacket) {
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

    public boolean isCommandFinished() {
        return this.state == ComQueryState.RESP_END || this.sqlType == MySQLCommand.COM_STMT_CLOSE;
    }

    public boolean isInteractive() {
        return this.state != ComQueryState.RESP_END || JudgeUtil.hasTrans(serverStatus) || JudgeUtil.hasFatch(serverStatus);
    }

    private PacketType resolveLongHalf(MySQLPacketInf packetInf, int offset, int limit, int totalLen) {
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

    private PacketType resolveRestCross(MySQLPacketInf packetInf, int offset, int limit, int totalLen) {
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

    public boolean crossBuffer(MySQLPacketInf packetInf) {
        if (packetInf.packetType == PacketType.LONG_HALF && !this.state.needFull) {
            packetInf.proxyBuffer.readIndex = packetInf.endPos;
            packetInf.updateState(packetInf.head, packetInf.startPos, packetInf.endPos, packetInf.pkgLength,
                    packetInf.pkgLength - (packetInf.endPos - packetInf.startPos), PacketType.REST_CROSS, packetInf.proxyBuffer);
            return true;
        } else {
            return false;
        }
    }

    private void checkNeedFull(MySQLPacketInf packetInf) {
        if ((state == ComQueryState.RESULTSET_ROW) && (packetInf.head == 0xfe) && packetInf.pkgLength < 0xffffff) {
            this.state = ComQueryState.RESULTSET_ROW_END;
        }
    }

    private void checkPacketId(byte packetId) {
        if (this.state != ComQueryState.DO_NOT) {
            if (nextPacketId != packetId){
                throw new RuntimeException("packetId should be " + nextPacketId + " that is not match " + packetId);
            }else {
            }
            ++nextPacketId;
        }
    }


    public void resolvePayloadType(MySQLPacketInf packetInf, boolean isPacketFinished) {
        int head = packetInf.head;
        switch (state) {
            case DO_NOT:
                return;
            case QUERY_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.sqlType = head;
                state = ComQueryState.FIRST_PACKET;
                return;
            }
            case FIRST_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 0xff) {
                    this.mysqlPacketType = MySQLPayloadType.ERROR;
                    state = ComQueryState.RESP_END;
                } else if (head == 0x00) {
                    if (sqlType == MySQLCommand.COM_STMT_PREPARE && packetInf.pkgLength == 16
                        //&& packetInf.packetId == 1
                    ) {
                        resolvePrepareOkPacket(packetInf, isPacketFinished);
                        return;
                    } else {
                        this.mysqlPacketType = MySQLPayloadType.OK;
                        this.serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                        state = ComQueryState.RESP_END;
                        return;
                    }
                } else if (head == 0xfb) {
                    state = ComQueryState.LOCAL_INFILE_REQUEST;
                    this.mysqlPacketType = LOCAL_INFILE_REQUEST;
                } else if (head == 0xfe) {
                    this.mysqlPacketType = EOF;
                    this.serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
                    state = ComQueryState.RESP_END;
                } else {
                    ProxyBuffer proxyBuffer = packetInf.proxyBuffer;
                    columnCount = proxyBuffer.getLenencInt(packetInf.startPos + 4);
                    this.mysqlPacketType = MySQLPayloadType.COLUMN_COUNT;
                    state = ComQueryState.COLUMN_DEFINITION;
                }
                return;
            }
            case COLUMN_DEFINITION: {
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryState.COLUMN_END_EOF : ComQueryState.RESULTSET_ROW;
                }
                this.mysqlPacketType = MySQLPayloadType.COULUMN_DEFINITION;
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
                    this.mysqlPacketType = MySQLPayloadType.BINARY_RESULTSET_ROW;
                } else if (head == 0xfe && packetInf.pkgLength < 0xffffff) {
                    resolveResultsetRowEnd(packetInf, isPacketFinished);
                } else if (head == MySQLPacket.ERROR_PACKET) {
                    this.mysqlPacketType = MySQLPayloadType.ERROR;
                    state = ComQueryState.RESP_END;
                } else {
                    this.mysqlPacketType = MySQLPayloadType.TEXT_RESULTSET_ROW;
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
            case LOCAL_INFILE_REQUEST:
                throw new UnsupportedOperationException("unsupport LOCAL INFILE!");
            case LOCAL_INFILE_FILE_CONTENT:
            case LOCAL_INFILE_EMPTY_PACKET:
            case LOCAL_INFILE_OK_PACKET:
            case RESP_END:
            default: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
            }
        }
    }

    private void resolveResultsetRowEnd(MySQLPacketInf packetInf, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (CLIENT_DEPRECATE_EOF) {
            this.mysqlPacketType = MySQLPayloadType.OK;
            serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
        } else {
            this.mysqlPacketType = EOF;
            serverStatus = EOFPacket.readStatus(packetInf.proxyBuffer);
        }
        if (JudgeUtil.hasMoreResult(serverStatus)) {
            state = ComQueryState.FIRST_PACKET;
        } else {
            state = ComQueryState.RESP_END;
        }
    }

    private void resolvePrepareOkPacket(MySQLPacketInf packetInf, boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        ProxyBuffer buffer = packetInf.proxyBuffer;
        buffer.readIndex = packetInf.startPos + 9;
        this.prepareFieldNum = (int) buffer.readFixInt(2);
        this.prepareParamNum = (int) buffer.readFixInt(2);
        this.mysqlPacketType = MySQLPayloadType.PREPARE_OK;
        if (this.prepareFieldNum == 0 && this.prepareParamNum == 0) {
            state = ComQueryState.RESP_END;
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
            this.mysqlPacketType = MySQLPayloadType.COULUMN_DEFINITION;
            this.state = ComQueryState.PREPARE_FIELD;
            if (prepareFieldNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    this.state = ComQueryState.PREPARE_FIELD_EOF;
                } else if (prepareParamNum > 0) {
                    this.state = ComQueryState.PREPARE_PARAM;
                } else {
                    this.state = ComQueryState.RESP_END;
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
            this.mysqlPacketType = MySQLPayloadType.COULUMN_DEFINITION;
            this.state = ComQueryState.PREPARE_PARAM;
            if (prepareParamNum == 0) {
                if (!CLIENT_DEPRECATE_EOF) {
                    state = ComQueryState.PREPARE_PARAM_EOF;
                    return;
                } else {
                    state = ComQueryState.RESP_END;
                    return;
                }
            } else {
                return;
            }
        } else if (prepareFieldNum == 0 && prepareParamNum == 0 && !CLIENT_DEPRECATE_EOF && ComQueryState.PREPARE_PARAM_EOF == state && head == 0xFE) {
            this.serverStatus = EOFPacket.readStatus(proxyBuf);
            this.mysqlPacketType = EOF;
            state = ComQueryState.RESP_END;
            return;
        }
        throw new RuntimeException("unknown state!");
    }

}
