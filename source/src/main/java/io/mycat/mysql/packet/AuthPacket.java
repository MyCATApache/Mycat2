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

import java.io.IOException;

import io.mycat.mysql.Capabilities;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.BufferUtil;

/**
 * From client to server during initial handshake.
 * <p>
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 *
 * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 *
 * @author mycat
 */
public class AuthPacket extends MySQLPacket {
    private static final byte[] FILLER = new byte[23];

    public long clientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra;// from FILLER(23)
    public String user;
    public byte[] password;
    public String database;

    public void read(ProxyBuffer byteBuffer) throws IOException {
        packetLength = (int) byteBuffer.readFixInt(3);
        packetId = byteBuffer.readByte();
        clientFlags = byteBuffer.readFixInt(4);
        maxPacketSize = byteBuffer.readFixInt(4);
        charsetIndex = byteBuffer.readByte();
        byteBuffer.skip(23);
        user = byteBuffer.readNULString();
        password = byteBuffer.readLenencBytes();
        if ((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) {
            database = byteBuffer.readNULString();
        }
    }

    public void write(ProxyBuffer buffer) {
    	this.write(buffer, calcPayloadSize());
    }
    public void write(ProxyBuffer buffer, int pkgSize) {
        buffer.writeFixInt(3, pkgSize);
        buffer.writeByte(packetId);
        buffer.writeFixInt(4, clientFlags);
        buffer.writeFixInt(4, maxPacketSize);
        buffer.writeByte((byte) charsetIndex);
        buffer.writeBytes(FILLER);
        if (user == null) {
            buffer.writeByte((byte) 0);
        } else {
            buffer.writeNULString(user);
        }
        if (password == null) {
            buffer.writeByte((byte) 0);
        } else {
            buffer.writeLenencBytes(password);
        }
        if (database == null) {
            buffer.writeByte((byte) 0);
        } else {
            buffer.writeNULString(database);
        }
    }

    @Override
    public int calcPayloadSize() {
        int size = 32;//4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Authentication Packet";
    }

}
