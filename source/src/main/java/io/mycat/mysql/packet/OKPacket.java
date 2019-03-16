/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mysql.packet;

import io.mycat.mysql.Capabilities;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.mysql.ServerStatus;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.BufferUtil;
import io.mycat.util.StringUtil;


/**
 * <pre>
 * @see https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
 * </pre>
 *
 * @author linxiaofang
 * @date 2018/11/12
 */
public final class OKPacket extends MySQLPacket {
    public static final byte OK_HEADER = 0x00;
    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    public byte header;
    public long affectedRows;
    public long lastInsertId;
    public int serverStatus;
    public int warningCount;
    public byte[] statusInfo;
    public SessionStateInfo sessionStateChanges;
    public byte[] message;
    CapabilityFlags capabilityFlags;

    public OKPacket() {
        capabilityFlags = new CapabilityFlags(Capabilities.CLIENT_PROTOCOL_41);
    }

    public OKPacket(int capabilities) {
        capabilityFlags = new CapabilityFlags(capabilities);
    }

    public void write(ProxyBuffer buffer) {
        buffer.writeFixInt(3, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeLenencInt(header);
        if (header == OK_HEADER) {
            buffer.writeLenencInt(affectedRows);
            buffer.writeLenencInt(lastInsertId);
            if (capabilityFlags.isClientProtocol41()) {
                buffer.writeFixInt(2, serverStatus);
                buffer.writeFixInt(2, warningCount);
            } else if (capabilityFlags.isKnowsAboutTransactions()) {
                buffer.writeFixInt(2, serverStatus);
            }
            if (capabilityFlags.isSessionVariableTracking()) {
                buffer.writeLenencBytes(statusInfo);
                if ((serverStatus & ServerStatus.STATE_CHANGED) != 0) {
                    sessionStateChanges.write(buffer);
                }
            } else {
                if (message != null) {
                    buffer.writeBytes(message);
                }
            }
        } else if (header < 0) {
            if (capabilityFlags.isClientProtocol41()) {
                buffer.writeFixInt(2, warningCount);
                buffer.writeFixInt(2, serverStatus);
            }
        }
    }

    public void read(ProxyBuffer buffer) {
        int startIndex = buffer.readIndex;
        packetLength = (int) buffer.readFixInt(3);
        packetId = buffer.readByte();
        header = buffer.readByte();
        if (header == OK_HEADER) {
            affectedRows = buffer.readLenencInt();
            lastInsertId = buffer.readLenencInt();
            if (capabilityFlags.isClientProtocol41()) {
                serverStatus = (int) buffer.readFixInt(2);
                warningCount = (int) buffer.readFixInt(2);

            } else if (capabilityFlags.isKnowsAboutTransactions()) {
                serverStatus = (int) buffer.readFixInt(2);
            }
            if (capabilityFlags.isSessionVariableTracking()) {
                statusInfo = buffer.readLenencBytes();
                if ((serverStatus & ServerStatus.STATE_CHANGED) != 0) {
                    sessionStateChanges = new SessionStateInfo();
                    sessionStateChanges.read(buffer);
                }
            } else {
                int remainLength = startIndex + packetLength + MySQLPacket.packetHeaderSize - buffer.readIndex;
                if (remainLength > 0) {
                    message = buffer.readBytes(remainLength);
                    System.out.println(new String(message));
                }
            }
        } else if (header < 0) {
            //EOF
            if (capabilityFlags.isClientProtocol41()) {
                warningCount = (int) buffer.readFixInt(2);
                serverStatus = (int) buffer.readFixInt(2);
            }
        }
    }

    public static int readServerStatus(ProxyBuffer buffer, CapabilityFlags capabilityFlags) {
        int startIndex = buffer.readIndex;
        int packetLength = (int) buffer.readFixInt(3);
        buffer.readByte(); //packetId = buffer.readByte();
        byte header = buffer.readByte();
        int serverStatus = 0;

        buffer.readLenencInt();//affectedRows
        buffer.readLenencInt();//lastInsertId
        if (capabilityFlags.isClientProtocol41() || capabilityFlags.isKnowsAboutTransactions()) {
            serverStatus = (int) buffer.readFixInt(2);
            //剩下的一次性读取完
            int remainLength = startIndex + packetLength + MySQLPacket.packetHeaderSize - buffer.readIndex;
            if (remainLength > 0) {
                buffer.skip(remainLength);
            }
            return serverStatus;
        }
        throw new java.lang.RuntimeException("OKPacket readServerStatus error " + StringUtil.dumpAsHex(buffer.getBytes(startIndex, 4 + packetLength)));
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        if (header == OK_HEADER) {
            i += BufferUtil.getLength(affectedRows);
            i += BufferUtil.getLength(lastInsertId);
            if (capabilityFlags.isClientProtocol41()) {
                i += 4;
            } else if (capabilityFlags.isKnowsAboutTransactions()) {
                i += 2;
            }
            if (capabilityFlags.isSessionVariableTracking()) {
                i += BufferUtil.getLength(statusInfo);
                if ((serverStatus & ServerStatus.STATE_CHANGED) != 0) {
                    i += sessionStateChanges.length();
                }
            } else {
                if (message != null) {
                    i += message.length;
                }
            }
        } else if (header < 0) {
            if (capabilityFlags.isClientProtocol41()) {
                i += 4;
            }
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }

    public class SessionStateInfo {
        public byte type;
        public byte[] data;

        public void read(ProxyBuffer buffer) {
            type = buffer.readByte();
            data = buffer.readLenencBytes();
            System.out.println(new String(data));
        }

        public void write(ProxyBuffer buffer) {
            buffer.writeByte(type);
            buffer.writeLenencBytes(data);
        }

        public int length() {
            return 1 + BufferUtil.getLength(data);
        }
    }

}
