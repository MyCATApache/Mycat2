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
package io.mycat.proxy.packet;


/**
 * https://mariadb.com/kb/en/library/err_packet/
 *
 * @author wuzhihui cjw
 */
public class ErrorPacketImpl implements ErrorPacket {
    public int errno;
    public int stage;
    public int maxStage;
    public int progress;
    public byte[] progress_info;
    public byte mark = ' ';
    public byte[] sqlState = DEFAULT_SQLSTATE;
    public String message;

    public void writePayload(MySQLPacket buffer) {
        buffer.writeByte((byte) 0xff);
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

    public void readPayload(MySQLPacket byteBuffer) {
        byte b = byteBuffer.readByte();
        assert (byte) 0xff == b;
        errno = (int) byteBuffer.readFixInt(2);
        if (errno == 0xFFFF) { /* progress reporting */
            stage = (int) byteBuffer.readFixInt(1);
            maxStage = (int) byteBuffer.readFixInt(1);
            progress = (int) byteBuffer.readFixInt(3);
            progress_info = byteBuffer.readLenencBytes();
        } else if (byteBuffer.getByte(byteBuffer.packetReadStartIndex()) == SQLSTATE_MARKER) {
            byteBuffer.skipInReading(1);
            mark = SQLSTATE_MARKER;
            sqlState = byteBuffer.readFixStringBytes(5);
        }
        message = byteBuffer.readEOFString();
    }

    public int getErrorStage() {
        return stage;
    }

    public void setErrorStage(int stage) {
        this.stage = stage;
    }

    public int getErrorMaxStage() {
        return maxStage;
    }

    public void setErrorMaxStage(int maxStage) {
        this.maxStage = maxStage;
    }

    public int getErrorProgress() {
        return progress;
    }

    public void setErrorProgress(int progress) {
        this.progress = progress;
    }

    public byte[] getErrorProgressInfo() {
        return progress_info;
    }

    public void setErrorProgressInfo(byte[] progress_info) {
        this.progress_info = progress_info;
    }

    public byte getErrorMark() {
        return mark;
    }

    public void setErrorMark(byte mark) {
        this.mark = mark;
    }

    public byte[] getErrorSqlState() {
        return sqlState;
    }

    public void setErrorSqlState(byte[] sqlState) {
        this.sqlState = sqlState;
    }

    public byte[] getErrorMessage() {
        return message.getBytes();
    }

    public void setErrorMessage(byte[] message) {
        this.message = new String(message);
    }
}
