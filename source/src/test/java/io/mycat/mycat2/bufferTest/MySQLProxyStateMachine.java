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
            }if (payloadFinishedInThisPacket && type == PacketType.FULL) {
                return PayloadType.FULL_PAYLOAD;
            }else if (type == PacketType.FULL){
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
        if (packetInf.packetType == PacketType.SHORT_HALF ||
                packetInf.packetType == PacketType.FULL ||
                packetInf.packetType == PacketType.FINISHED_CROSS) {
            if (totalLen == 4) {
                packetInf.pkgLength = ParseUtil.getPacketLength(buffer, offset);
                if (packetInf.pkgLength == 4){
                    int packetId = buffer.get(offset + 3) & 0xff;
                    if (++lastPacketId != packetId) {
                        throw new RuntimeException("packetId should be " + lastPacketId + " that is not match " + packetId);
                    }
                    packetInf.resetByPacketId(packetId);
                    this.payloadFinishedInThisPacket = (packetInf.pkgLength) != 0xffffff + 4;
                    return PacketType.FULL;
                }
            }
            if (totalLen < LONG_HALF_MIN_LENGTH) {
                mysqlPacketType = MySQLRespPacketType.UNKNOWN;
                return packetInf.packetType = PacketType.SHORT_HALF;
            }
            int packetId = buffer.get(offset + 3) & 0xff;
            if (++lastPacketId != packetId) {
                throw new RuntimeException("packetId should be " + lastPacketId + " that is not match " + packetId);
            }
            packetInf.resetByPacketId(packetId);

            packetInf.pkgLength = ParseUtil.getPacketLength(buffer, offset);

            packetInf.head = buffer.get(offset + ParseUtil.msyql_packetHeaderSize);
            this.payloadFinishedInThisPacket = (packetInf.pkgLength) != 0xffffff + 4;
            packetInf.startPos = offset;
            boolean isPacketFinished = totalLen >= packetInf.pkgLength;
            boolean isFullFirstPacket = state == ComQueryRespState.FIRST_PACKET && isPacketFinished;
            boolean isFullEndingPacket = (state == ComQueryRespState.RESULTSET_ROW || state == ComQueryRespState.PREPARE_RESPONSE) &&
                    packetInf.head == (byte) 0xfe && packetInf.pkgLength < 0xffffff + 4 && isPacketFinished;
            boolean isOtherPacketLeaveShortHalf = packetInf.head != (byte) 0xfe && packetInf.packetType == PacketType.SHORT_HALF && !isFullFirstPacket && !isFullEndingPacket;
            if (isFullFirstPacket || isFullEndingPacket || isOtherPacketLeaveShortHalf) {
                resolveResponse(packetInf, offset, totalLen, packetInf.pkgLength, packetInf.head, isFullEndingPacket);
            } ////未达到整包的eof err ok
            packetInf.packetType = PacketType.LONG_HALF;
        }
        if (packetInf.packetType == PacketType.LONG_HALF) {
            if (totalLen >= packetInf.pkgLength||totalLen== 0xffffff&&packetInf.pkgLength == 0xffffff+4) {
                packetInf.endPos = offset + packetInf.pkgLength;
                return packetInf.packetType = PacketType.FULL;
            }
            if (offset == 0 && packetInf.pkgLength > limit && totalLen > buffer.capacity()) {
                packetInf.needExpandBuffer = true;
            }
            packetInf.endPos = limit;
            return packetInf.packetType = PacketType.LONG_HALF;
        }
        if (packetInf.packetType == PacketType.REST_CROSS) {
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
        throw new RuntimeException("unknown state!");
    }

    private void resolveResponse(PacketInf packetInf, int offset, int totalLen, int packageLength, int head, boolean isFullEndingPacketInResultSet) {
        switch (state) {
            case FIRST_PACKET: {
                if (head == MySQLPacket.ERROR_PACKET) {
                    resolveErrPacketInFirstPacket(offset, totalLen, packageLength);
                } else if (head == MySQLPacket.OK_PACKET) {
                    resolveOkPacketInFirstPacket(offset, totalLen, packageLength);
                } else if (head == 0xfb) {
                    throw new UnsupportedOperationException("unsupport LOCAL INFILE!");
                } else if (head == MySQLPacket.EOF_PACKET) {
                    resolveEOFPacketInFirstPacket(offset, totalLen, packageLength);
                }//Column count packet
                resokveColumnCountPacketInFirstPacket(packetInf.proxyBuffer, offset, totalLen, packageLength);
                return;
            }
            case COLUMN_DEFINITION: {
                --columnCount;
                if (columnCount == 0) {
                    this.state = !this.CLIENT_DEPRECATE_EOF ? ComQueryRespState.COLUMN_END_EOF : ComQueryRespState.RESULTSET_ROW;
                }
                return;
            }
            case COLUMN_END_EOF: {
                this.state = ComQueryRespState.RESULTSET_ROW;
                return;
            }
            case RESULTSET_ROW: {
                if (head == 0x00) {
                    //binary resultset row
                } else if (isFullEndingPacketInResultSet) {
                    if (CLIENT_DEPRECATE_EOF) {
                        //ok
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    } else {
                        //eof
                        serverStatus = OKPacket.readServerStatus(packetInf.proxyBuffer, capabilityFlags);
                    }
                    if (JudgeUtil.hasMoreResult(serverStatus)) {
                        state = ComQueryRespState.COLUMN_DEFINITION;
                    } else {
                        willBeFinished = true;
                    }
                } else if (head == MySQLPacket.ERROR_PACKET) {

                } else {
                    //text resultset row
                }
                break;
            }
            case PREPARE_RESPONSE: {
                resolvePrepareResponse(packetInf.proxyBuffer, head);
            }
        }
    }

    private void resolvePrepareResponse(ProxyBuffer proxyBuf, int head) {
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

        public PacketInf(ProxyBuffer proxyBuffer) {
            this.proxyBuffer = proxyBuffer;
        }

        public void resetByPacketId(int packetId) {
            this.head = 0;
            this.packetId = packetId;
            this.startPos = 0;
            this.endPos = 0;
            this.pkgLength = 0;
            this.remainsBytes = 0;
            this.packetType = PacketType.SHORT_HALF;
            this.needExpandBuffer = false;
        }

        public void crossBuffer() {
            if (this.packetType == PacketType.LONG_HALF) {
                this.packetType = PacketType.REST_CROSS;
                PacketInf curMSQLPackgInf = this;
                if (curMSQLPackgInf.remainsBytes == 0 && !ParseUtil.validateHeader(curMSQLPackgInf.startPos, curMSQLPackgInf.endPos)) {
                    throw new UnsupportedOperationException("");
                }
                this.remainsBytes = this.pkgLength
                        - (this.endPos - this.startPos);
                this.proxyBuffer.readIndex = this.endPos;
            } else {
                throw new UnsupportedOperationException("crossBuffer is only in LongHalf");
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
