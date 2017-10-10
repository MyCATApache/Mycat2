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
 * From Server To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 * 
 * <pre>
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Field_Packet
 * </pre>
 * 
 * @author mycat
 */
public class FieldPacket extends MySQLPacket {
	private static final byte[] DEFAULT_CATALOG = "def".getBytes();
	private static final byte[] FILLER = new byte[2];

	public byte[] catalog = DEFAULT_CATALOG;
	public byte[] db;
	public byte[] table;
	public byte[] orgTable;
	public byte[] name;
	public byte[] orgName;
	public int charsetIndex;
	public long length;
	public int type;
	public int flags;
	public byte decimals;
	public byte[] definition;

	/**
	 * 把字节数组转变成FieldPacket
	 */
	public void read(ProxyBuffer buffer) {
		this.packetLength = (int) buffer.readFixInt(3);
		this.packetId = buffer.readByte();
		readBody(buffer);
	}



	@Override
	public  void write(ProxyBuffer buffer) {
		int size = calcPacketSize();
//		buffer.writeFixInt(3, size);
//		buffer.writeByte(packetId);
		writeBody(buffer);
	}

	@Override
	public int calcPacketSize() {
		int size = (catalog == null ? 1 : ProxyBuffer.getLenencLength(catalog));
		size += (db == null ? 1 : ProxyBuffer.getLenencLength(db));
		size += (table == null ? 1 : ProxyBuffer.getLenencLength(table));
		size += (orgTable == null ? 1 : ProxyBuffer.getLenencLength(orgTable));
		size += (name == null ? 1 : ProxyBuffer.getLenencLength(name));
		size += (orgName == null ? 1 : ProxyBuffer.getLenencLength(orgName));
		size += 13;// 1+2+4+1+2+1+2
		if (definition != null) {
			size += ProxyBuffer.getLenencLength(definition.length);
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Field Packet";
	}

	private void readBody(ProxyBuffer buffer) {
		int startPos = buffer.readIndex;
		this.catalog = buffer.readLenencBytes();
		this.db = buffer.readLenencBytes();
		this.table = buffer.readLenencBytes();
		this.orgTable = buffer.readLenencBytes();
		this.name = buffer.readLenencBytes();
		this.orgName = buffer.readLenencBytes();
		buffer.skip(1);
		this.charsetIndex = (int) buffer.readFixInt(2);
		this.length = (int) buffer.readFixInt(4);
		this.type = buffer.readByte() & 0xff;
		this.flags =(int) buffer.readFixInt(2);
		this.decimals =buffer.readByte();
		buffer.skip(FILLER.length);
		
		if ( buffer.readIndex - startPos < this.packetLength) {
			this.definition = buffer.readBytes(packetLength - buffer.readIndex);
		}
	}

	private void writeBody(ProxyBuffer buffer) {
		byte[] nullVal = {0};
		
		buffer.writeFixInt(3, 0);
		buffer.writeByte(packetId);
		
		int writeIndexPos = buffer.writeIndex;
		buffer.writeLenencBytes(catalog, nullVal);
		buffer.writeLenencBytes(db, nullVal);
		buffer.writeLenencBytes(table, nullVal);
		buffer.writeLenencBytes(orgTable, nullVal);
		buffer.writeLenencBytes(name, nullVal);
		buffer.writeLenencBytes(orgName, nullVal);
		buffer.writeByte((byte) 0x0C);
		buffer.writeFixInt(2, charsetIndex);
		buffer.writeFixInt(4, length);
		buffer.writeByte((byte) (type & 0xff));
		buffer.writeFixInt(2, flags);
		
		buffer.writeByte(decimals);
        buffer.writeByte((byte)0x00);
        buffer.writeByte((byte)0x00);
		if (definition != null) {
			buffer.writeBytes(definition);
		}
		buffer.putFixInt(writeIndexPos - 4, 3, (buffer.writeIndex - writeIndexPos));
		System.out.println("length " + (buffer.writeIndex - writeIndexPos));
	}


}