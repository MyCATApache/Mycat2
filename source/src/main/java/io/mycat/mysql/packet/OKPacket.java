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
import io.mycat.util.BufferUtil;

/**
 * From Server To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 * 
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 * 
 * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#EOF_Packet
 * </pre>
 * @todo 错误的注释
 * @author mycat
 */
public final class OKPacket extends MySQLPacket {

	public static final byte PKG_TYPE = MySQLPacket.OK_PACKET;

	public static final byte FIELD_COUNT = 0x00;
	public static final byte[] OK = new byte[] { 7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };

	public byte fieldCount = FIELD_COUNT;
	public long affectedRows;
	public long insertId;
	public int serverStatus;
	public int warningCount;
	public byte[] message;

	public void write(ProxyBuffer buffer) {
		buffer.writeFixInt(3, calcPacketSize());
		buffer.writeByte(packetId);
		buffer.writeLenencInt(fieldCount);
		buffer.writeLenencInt(affectedRows);
		buffer.writeLenencInt(insertId);
		buffer.writeFixInt(2, serverStatus);
		buffer.writeFixInt(2, warningCount);
		if (message != null) {
			buffer.writeLenencString(message);
		}
	}

	public void read(ProxyBuffer buffer) {
		int index = buffer.readIndex;
		packetLength = (int) buffer.readFixInt(3);
		packetId = buffer.readByte();
		fieldCount = buffer.readByte();
		affectedRows = buffer.readLenencInt();
		insertId = buffer.readLenencInt();
		serverStatus = (int) buffer.readFixInt(2);
		warningCount = (int) buffer.readFixInt(2);
		if (index + packetLength + MySQLPacket.packetHeaderSize - buffer.readIndex > 0) {
			this.message = buffer.readLenencStringBytes();
		}
	}

	@Override
	public int calcPacketSize() {
		int i = 1;
		i += BufferUtil.getLength(affectedRows);
		i += BufferUtil.getLength(insertId);
		i += 4;
		if (message != null) {
			i += BufferUtil.getLength(message);
		}
		return i;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL OK Packet";
	}

}
