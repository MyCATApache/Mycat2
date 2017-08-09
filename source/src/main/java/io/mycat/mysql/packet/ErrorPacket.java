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

import io.mycat.mycat2.MySQLPackageInf;
import io.mycat.proxy.ProxyBuffer;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 * @author mycat
 */
public class ErrorPacket extends MySQLPacket {
   
    private static final byte SQLSTATE_MARKER = (byte) '#';
    private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    public byte pkgType = MySQLPacket.ERROR_PACKET;
    public int errno;
    public byte mark = SQLSTATE_MARKER;
    public byte[] sqlState = DEFAULT_SQLSTATE;
    public String message;

  
    public void read(ProxyBuffer byteBuffer) {
        packetLength =(int) byteBuffer.readFixInt(3);
        packetId =byteBuffer.readByte();
        pkgType =byteBuffer.readByte();
        errno = (int) byteBuffer.readFixInt(2);
        if (byteBuffer.readState.hasRemain() && (byteBuffer.getByte(byteBuffer.readState.optPostion) == SQLSTATE_MARKER)) {
        	byteBuffer.skip(1);
            sqlState = byteBuffer.readBytes(5);
        }
        message = byteBuffer.readNULString();
    }

    public void write(ProxyBuffer buffer){
        buffer.writeFixInt(3,calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(pkgType);
        buffer.writeFixInt(2,errno);
        buffer.writeByte(mark);
        buffer.writeBytes(sqlState);
        if (message != null) {
            buffer.writeNULString(message);

        }
    }

    @Override
    public int calcPacketSize() {
        int size = 9;// 1 + 2 + 1 + 5
        if (message != null) {
            size += message.length()+1;
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Error Packet";
    }

}
