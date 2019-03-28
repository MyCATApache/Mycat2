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
 * https://mariadb.com/kb/en/library/err_packet/
 * @author wuzhihui cjw
 *
 */
public class ErrorPacket extends MySQLPacket {

	private static final byte SQLSTATE_MARKER = (byte) '#';
	private static final String DEFAULT_SQLSTATE = "HY000";

	public byte pkgType = (byte)MySQLPacket.ERROR_PACKET;
	public int errno;
	public int stage;
	public int maxStage;
	public int progress;
	public String progress_info;
	public byte mark = ' ';
	public String sqlState = DEFAULT_SQLSTATE;
	public String message;

	@Override
	public void writePayload(ProxyBuffer buffer) {
		buffer.writeByte(pkgType);
		buffer.writeFixInt(2, errno);
		if (errno == 0xFFFF) { /* progress reporting */
			buffer.writeFixInt(1, stage);
			buffer.writeFixInt(1, maxStage);
			buffer.writeFixInt(3, progress);
			buffer.writeLenencString(progress_info);

		} else if (mark == SQLSTATE_MARKER) {
			buffer.writeByte(mark);
			buffer.writeFixString(sqlState);
		}
		buffer.writeEOFString(message);
	}

	@Override
	public void readPayload(ProxyBuffer byteBuffer) {
		pkgType = byteBuffer.readByte();
		errno = (int) byteBuffer.readFixInt(2);
		if (errno == 0xFFFF) { /* progress reporting */
			stage = (int) byteBuffer.readFixInt(1);
			maxStage = (int) byteBuffer.readFixInt(1);
			progress = (int) byteBuffer.readFixInt(3);
			progress_info = byteBuffer.readLenencString();
		} else if (byteBuffer.getByte(byteBuffer.readIndex) == SQLSTATE_MARKER) {
			byteBuffer.skip(1);
			mark = SQLSTATE_MARKER;
			sqlState = byteBuffer.readFixString(5);
		}
		message = byteBuffer.readEOFString();
	}

	@Override
	public int calcPayloadSize() {
		int size = 1 + 2;// pkgType+errorcode
		if (errno == 0xFFFF) { /* progress reporting */
			size += 1 + 1 + 3 + ProxyBuffer.getLenencLength( progress_info.length())+progress_info.length();
		} else if (mark == SQLSTATE_MARKER) {
			size += 1 + 5;// mark+sqlstate
		}
		if (message != null){
			size += message.length();
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Error Packet";
	}

}
