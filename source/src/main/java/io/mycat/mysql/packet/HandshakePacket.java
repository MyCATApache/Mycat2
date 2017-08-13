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

import io.mycat.proxy.ProxyBuffer;

/**
 * From server to client during initial handshake.
 * <p>
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version
 * n (Null-Terminated String)   server_version
 * 4                            thread_id
 * 8                            scramble_buff
 * 1                            (filler) always 0x00
 * 2                            server_capabilities
 * 1                            server_language
 * 2                            server_status
 * 13                           (filler) always 0x00 ...
 * 13                           rest of scramble_buff (4.1)
 *
 * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
 * </pre>
 *
 * @author mycat
 */
public class HandshakePacket extends MySQLPacket {
    private static final byte[] FILLER_13 = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public int serverStatus;
    public byte[] restOfScrambleBuff;


    public void read(ProxyBuffer buffer) {
        packetLength = (int) buffer.readFixInt(3);
        packetId = buffer.readByte();
        protocolVersion = buffer.readByte();
        serverVersion = buffer.readNULString().getBytes();
        threadId = buffer.readFixInt(4);
        seed = buffer.readNULString().getBytes();
        serverCapabilities = (int) buffer.readFixInt(2);
        serverCharsetIndex = buffer.readByte();
        serverStatus = (int) buffer.readFixInt(2);
        buffer.skip(13);
        restOfScrambleBuff = buffer.readNULString().getBytes();
    }

    public void write(ProxyBuffer buffer) {
    	int pkgSize=calcPacketSize();
    	//进行将握手包，写入至ProxyBuffer中,将write的opt指针进行相应用修改
    	
        buffer.writeFixInt(3, pkgSize);
        buffer.writeByte(packetId);
        buffer.writeByte(protocolVersion);
        buffer.writeNULString(new String(serverVersion));
        buffer.writeFixInt(4, threadId);
        buffer.writeNULString(new String(seed));
        buffer.writeFixInt(2, serverCapabilities);
        buffer.writeByte(serverCharsetIndex);
        buffer.writeFixInt(2, serverStatus);
        buffer.writeBytes(FILLER_13);
        buffer.writeNULString(new String(restOfScrambleBuff));
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += serverVersion.length;// n
        size += 5;// 1+4
        size += seed.length;// 8
        size += 19;// 1+2+1+2+13
        size += restOfScrambleBuff.length;// 12
        size += 1;// 1
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Handshake Packet";
    }

}
