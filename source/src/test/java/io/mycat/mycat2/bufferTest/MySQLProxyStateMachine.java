package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ParseUtil;

import java.nio.ByteBuffer;
import java.util.List;

import static io.mycat.util.ParseUtil.msyql_packetHeaderSize;
import static io.mycat.util.ParseUtil.mysql_packetTypeSize;

public class MySQLProxyStateMachine {
    int sqlType = -1;
    int prepareFieldNum = 0;
    int prepareParamNum = 0;
    long columnCount = 0;
    int serverStatus = 0;
    int lastPacketId = 0;
    ComQueryRespState state = ComQueryRespState.FIRST_PACKET;
    boolean willBeFinished = false;

    MySQLRespPacketType mysqlPacketType = MySQLRespPacketType.UNKNOWN;
    boolean payloadFinishedInThisPacket = false;


    CapabilityFlags capabilityFlags = MySQLSession.getClientCapabilityFlags();
    boolean CLIENT_DEPRECATE_EOF = true;


    public static final int LONG_HALF_MIN_LENGTH = msyql_packetHeaderSize + mysql_packetTypeSize;
    public static final int RESULT_MIN_LENGTH = 13;


    public PayloadType resolveFullPayload(PacketInf packetInf, List<PacketInf> list) {
        ProxyBuffer proxyBuf = packetInf.proxyBuffer;
        while (proxyBuf.writeIndex != proxyBuf.readIndex) {
            PacketType type = resolveMySQLPackage(packetInf);
            if (type == PacketType.FULL) {//终止条件
                list.add(packetInf);
                if (this.payloadFinishedInThisPacket) {
                    return PayloadType.FULL_PAYLOAD;
                } else {
                    return PayloadType.TYPE_PAYLOAD;
                }
            } else if (packetInf.needExpandBuffer) {
                list.add(packetInf);
                break;
            } else if (type == PacketType.LONG_HALF) {
                return PayloadType.TYPE_PAYLOAD;
            } else {
                break;
            }
        }
        return PayloadType.HALF_PAYLOAD;
    }

    public PayloadType resolveCrossBufferFullPayload(PacketInf packetInf) {
        ProxyBuffer proxyBuf = packetInf.proxyBuffer;
        while (proxyBuf.writeIndex != proxyBuf.readIndex) {
            PacketType type = resolveMySQLPackage(packetInf);
            if (payloadFinishedInThisPacket && type == PacketType.FINISHED_CROSS) {
                return PayloadType.FULL_PAYLOAD;
            } else if (type == PacketType.LONG_HALF) {
                packetInf.crossBuffer();
                return PayloadType.TYPE_PAYLOAD;
            } else if (type == PacketType.SHORT_HALF) {
                return PayloadType.HALF_PAYLOAD;
            } else if (type == PacketType.REST_CROSS) {
                return PayloadType.REST_CROSS_PAYLOAD;
            }
            if (payloadFinishedInThisPacket && type == PacketType.FULL) {
                return PayloadType.FULL_PAYLOAD;
            } else if (type == PacketType.FULL) {
                return PayloadType.TYPE_PAYLOAD;
            }
            return PayloadType.TYPE_PAYLOAD;
        }
        throw new RuntimeException("unknown state!");
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
                    packetInf.resetByPacketId(packetId);
                    packetInf.pkgLength = ParseUtil.getPacketLength(buffer, offset);
                    boolean isCrossPacket = this.payloadFinishedInThisPacket;
                    this.payloadFinishedInThisPacket = (packetInf.pkgLength) != 0xffffff + 4;
                    boolean isPacketFinished = totalLen >= packetInf.pkgLength;
                    if (isCrossPacket) {
                        return resolveCrossPacket(packetInf, offset, limit, totalLen);
                    }
                    if (totalLen > 4) {
                        return resolveShort2FullOrLongHalf(packetInf, offset, limit, buffer, isPacketFinished);
                    } else {// totalLen == 4 but not CrossPacket;
                        throw new RuntimeException("unknown state!");
                    }
                } else {
                    return PacketType.SHORT_HALF;
                }
            }
            case LONG_HALF:
                return resolveLongHalf(packetInf, offset, limit, totalLen, buffer);
            case REST_CROSS:
                return resolveRestCross(packetInf, offset, limit, totalLen);
            default:
                throw new RuntimeException("unknown state!");

        }
    }

    private PacketType resolveShort2FullOrLongHalf(PacketInf packetInf, int offset, int limit, ByteBuffer buffer, boolean isPacketFinished) {
        packetInf.head = buffer.get(offset + 4) & 0xff;
        judgeMetaDataPacket(packetInf);
        if (!packetInf.isMetaData) {
            resolvePayloadType(packetInf, isPacketFinished);
        }
        packetInf.startPos = offset;
        if (isPacketFinished) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (packetInf.isMetaData) {
                resolvePayloadType(packetInf, true);
            }
            return packetInf.packetType = PacketType.FULL;
        } else {
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
    }

    private void judgeMetaDataPacket(PacketInf packetInf) {
        switch (state) {
            case COLUMN_DEFINITION:
                packetInf.isMetaData = false;
                break;
            case PREPARE_RESPONSE:
            case FIRST_PACKET:
            case COLUMN_END_EOF:
                packetInf.isMetaData = true;
                break;
            case RESULTSET_ROW:
                packetInf.isMetaData = (packetInf.head == 0xfe) && packetInf.pkgLength < 0xffffff;
                break;
            default:
                throw new RuntimeException("unknown state!");
        }
    }

    private void checkPacketId(int packetId) {
        if (++lastPacketId != packetId) {
            throw new RuntimeException("packetId should be " + lastPacketId + " that is not match " + packetId);
        }
    }

    private PacketType resolveCrossPacket(PacketInf packetInf, int offset, int limit, int totalLen) {
        packetInf.startPos = offset;
        if (totalLen >= packetInf.pkgLength) {
            packetInf.endPos = offset + packetInf.pkgLength;
            return PacketType.FULL;
        } else {
            packetInf.endPos = limit;
            return PacketType.LONG_HALF;
        }
    }

    private void resolvePayloadType(PacketInf packetInf, boolean isPacketFinished) {
        int head = packetInf.head;
        switch (state) {
            case FIRST_PACKET: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                if (head == 0xff) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    willBeFinished = true;
                } else if (head == 0x00) {
                    this.mysqlPacketType = MySQLRespPacketType.OK;
                    willBeFinished = true;
                } else if (head == 0xfb) {
                    throw new UnsupportedOperationException("unsupport LOCAL INFILE!");
                } else if (head == 0xfe) {
                    this.mysqlPacketType = MySQLRespPacketType.EOF;
                    willBeFinished = true;
                }//Column count packet
                return;
            }
            case COLUMN_DEFINITION: {
                this.mysqlPacketType = MySQLRespPacketType.COULUMN_DEFINITION;
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryRespState.COLUMN_END_EOF : ComQueryRespState.RESULTSET_ROW;
                }
                return;
            }
            case COLUMN_END_EOF: {
                if (!isPacketFinished) throw new RuntimeException("unknown state!");
                this.state = ComQueryRespState.RESULTSET_ROW;
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    //binary resultset row
                    this.mysqlPacketType = MySQLRespPacketType.BINARY_RESULTSET_ROW;
                } else if (head == 0xfe) {
                    if (!isPacketFinished) throw new RuntimeException("unknown state!");
                    if (CLIENT_DEPRECATE_EOF) {
                        this.mysqlPacketType = MySQLRespPacketType.OK;
                        //ok
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    } else {
                        this.mysqlPacketType = MySQLRespPacketType.EOF;
                        //eof
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    }
                    if (JudgeUtil.hasMoreResult(serverStatus)) {
                        state = ComQueryRespState.COLUMN_DEFINITION;
                    } else {
                        willBeFinished = true;
                    }
                } else if (head == MySQLPacket.ERROR_PACKET) {
                    this.mysqlPacketType = MySQLRespPacketType.ERROR;
                    willBeFinished = true;
                } else {
                    this.mysqlPacketType = MySQLRespPacketType.TEXT_RESULTSET_ROW;
                    //text resultset row
                }
                break;
            }
            case PREPARE_RESPONSE: {
                this.mysqlPacketType = MySQLRespPacketType.PREPARE_OK;
                resolvePrepareResponse(packetInf.proxyBuffer, head,isPacketFinished);
            }
        }
    }

    private PacketType resolveLongHalf(PacketInf packetInf, int offset, int limit, int totalLen, ByteBuffer buffer) {
        packetInf.startPos = offset;
        if (totalLen >= packetInf.pkgLength) {
            packetInf.endPos = offset + packetInf.pkgLength;
            if (packetInf.isMetaData) {
                resolvePayloadType(packetInf,true);
            }
            return packetInf.packetType = PacketType.FULL;
        }
        if (offset == 0 && packetInf.pkgLength > limit && totalLen > buffer.capacity()) {
            packetInf.needExpandBuffer = true;
        }
        packetInf.endPos = limit;
        return packetInf.packetType = PacketType.LONG_HALF;
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

    private void resolvePrepareResponse(ProxyBuffer proxyBuf, int head,boolean isPacketFinished) {
        if (!isPacketFinished) throw new RuntimeException("unknown state!");
        if (prepareFieldNum > 0) {
            prepareFieldNum--;
        } else if (prepareFieldNum == 0) {
            if (CLIENT_DEPRECATE_EOF) {
                prepareFieldNum = -1;
            } else if (head == MySQLPacket.EOF_PACKET) {
                prepareFieldNum = -1;
                serverStatus = OKPacket.readServerStatus(proxyBuf, capabilityFlags);
            }
        } else if (prepareParamNum > 0) {
            prepareParamNum--;
        }
        if (prepareParamNum == 0) {
            if (CLIENT_DEPRECATE_EOF) {
                prepareParamNum = -1;
            } else if (head == MySQLPacket.EOF_PACKET) {
                prepareParamNum = -1;
                serverStatus = OKPacket.readServerStatus(proxyBuf, capabilityFlags);
            }
        }
        this.willBeFinished = prepareFieldNum == -1 && prepareParamNum == -1;
    }

    private void resokveColumnCountPacketInFirstPacket(ProxyBuffer proxyBuf, int offset, int totalLen, int packageLength) {
        int startPos = offset;
        int endPos = offset + packageLength;
        this.columnCount = proxyBuf.getLenencInt(LONG_HALF_MIN_LENGTH);
        this.state = ComQueryRespState.COLUMN_DEFINITION;
    }

    private void resolveOkPacketInFirstPacket(int offset, int totalLen, int packageLength) {
        int startPos = offset;
        int endPos = offset + packageLength;
        if (sqlType == MySQLCommand.COM_STMT_PREPARE) {
            state = ComQueryRespState.PREPARE_RESPONSE;
        }
    }

    private void resolveEOFPacketInFirstPacket(int offset, int totalLen, int packageLength) {

    }

    private void resolveErrPacketInFirstPacket(int offset, int totalLen, int packageLength) {
        int startPos = offset;
        int endPos = offset + packageLength;
        this.mysqlPacketType = MySQLRespPacketType.ERROR;
        this.willBeFinished = true;
    }

    static enum Direction {
        REQUEST, RESPONSE
    }

    static enum ComQueryRespState {
        FIRST_PACKET,
        COLUMN_DEFINITION,
        PREPARE_RESPONSE,
        COLUMN_END_EOF,
        RESULTSET_ROW
    }

    public static class PacketInf {
        public int head;
        public int packetId;
        public int startPos;
        public int endPos;
        public int pkgLength;
        public int remainsBytes;//还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
        public PacketType packetType = PacketType.SHORT_HALF;
        public boolean needExpandBuffer;
        public ProxyBuffer proxyBuffer;
        private boolean isMetaData = false;

        public PacketInf(ProxyBuffer proxyBuffer) {
            this.proxyBuffer = proxyBuffer;
        }

        public void resetByPacketId(int packetId) {
            this.packetId = packetId;
            this.pkgLength = 0;
            this.head = 0;
            this.startPos = 0;
            this.endPos = 0;
            this.remainsBytes = 0;
            this.packetType = PacketType.SHORT_HALF;
            this.needExpandBuffer = false;
            this.isMetaData = false;
        }

        public boolean crossBuffer() {
            if (this.packetType == PacketType.LONG_HALF && !isMetaData) {
                this.packetType = PacketType.REST_CROSS;
                PacketInf curMSQLPackgInf = this;
                if (curMSQLPackgInf.remainsBytes == 0 && !ParseUtil.validateHeader(curMSQLPackgInf.startPos, curMSQLPackgInf.endPos)) {
                    throw new UnsupportedOperationException("");
                }
                this.remainsBytes = this.pkgLength - (this.endPos - this.startPos);
                this.proxyBuffer.readIndex = this.endPos;
                return true;
            } else {
                return false;
            }
        }

        public void markRead() {
            if (packetType == PacketType.FULL || packetType == PacketType.REST_CROSS || packetType == PacketType.FINISHED_CROSS) {
                this.proxyBuffer.readIndex = endPos;
            } else {
                throw new UnsupportedOperationException("markRead is only in FULL or REST_CROSS");
            }
        }
    }

    public static enum PacketType {
        FULL, LONG_HALF, SHORT_HALF, REST_CROSS, FINISHED_CROSS
    }

    public static enum PayloadType {
        HALF_PAYLOAD, TYPE_PAYLOAD, FULL_PAYLOAD, REST_CROSS_PAYLOAD, FINISHED_CROSS_PAYLOAD
    }
}
