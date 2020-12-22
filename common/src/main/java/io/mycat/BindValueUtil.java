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
package io.mycat;

import io.mycat.beans.mysql.packet.MySQLPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author mycat
 */
public class BindValueUtil {

    public static final void read(MySQLPacket mm, BindValue bv, Charset charset) {
        switch (bv.type & 0xff) {
            case MysqlDefs.FIELD_TYPE_BIT:
                bv.value = mm.readLenencBytes();
                break;
            case MysqlDefs.FIELD_TYPE_TINY:
                bv.byteBinding = mm.readByte();
                break;
            case MysqlDefs.FIELD_TYPE_SHORT:
                bv.shortBinding = (short) mm.readFixInt(2);
                break;
            case MysqlDefs.FIELD_TYPE_LONG:
                bv.intBinding = (int) mm.readFixInt(4);
                break;
            case MysqlDefs.FIELD_TYPE_LONGLONG:
                bv.longBinding = mm.readLong();
                break;
            case MysqlDefs.FIELD_TYPE_FLOAT:
                bv.floatBinding = mm.readFloat();
                break;
            case MysqlDefs.FIELD_TYPE_DOUBLE:
                bv.doubleBinding = mm.readDouble();
                break;
            case MysqlDefs.FIELD_TYPE_TIME:
                bv.value = mm.readTime();
                break;
            case MysqlDefs.FIELD_TYPE_DATE:
            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                bv.value = mm.readDate();
                break;
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
                bv.value = mm.readLenencString();
                break;
            case MysqlDefs.FIELD_TYPE_DECIMAL:
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                bv.value = mm.readBigDecimal();
                if (bv.value == null) {
                    bv.isNull = true;
                }
                break;
            case MysqlDefs.FIELD_TYPE_BLOB:
                byte[] vv = mm.readLenencBytes();
                if (vv == null) {
                    bv.isNull = true;
                } else {
                    //vv.length >= 0
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    try {
                        out.write(vv);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.type);
                    }
                    bv.value = out;
                }
                break;
            default:
                throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.type);
        }
        bv.isSet = true;
    }

}