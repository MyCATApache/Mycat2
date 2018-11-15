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
 * @author wuzhihui
 */
public abstract class MySQLPacket {

    public static int packetHeaderSize = 4;

    // 后端报文类型
//    public static final byte REQUEST_FILE_FIELD_COUNT = (byte) 251;
    public static final byte OK_PACKET = 0;
    public static final byte ERROR_PACKET = (byte) 0xFF;
    public static final byte EOF_PACKET = (byte) 0xFE;
    public static int NOT_OK_EOF_ERR = Integer.MAX_VALUE;

//	public static final byte FIELD_EOF_PACKET = (byte) 0xFE;
//	public static final byte ROW_EOF_PACKET = (byte) 0xFE;
//	public static final byte AUTH_PACKET = 1;
//	public static final byte QUIT_PACKET = 2;
    /**
     * Mycat heartbeat
     */
    public static final byte COM_HEARTBEAT = 64;

    /**
     * 此用来标识当前为查询的响应包，mysql本身无此定义，但由于查询结果集无法统一标识，在程序中做出此标识
     */
    public static final int RESULTSET_PACKET = 12032;

    public int packetLength;
    public byte packetId;

    /**
     * 计算数据包大小，不包含包头长度。
     */
    public abstract int calcPacketSize();

    /**
     * 取得数据包信息
     */
    protected abstract String getPacketInfo();

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()).append("{length=").append(packetLength).append(",id=")
                .append(packetId).append('}').toString();
    }

    /**
     * 写入到Buffer里（为了发送）
     *
     * @param buffer
     */
    public abstract void write(ProxyBuffer buffer);
}
