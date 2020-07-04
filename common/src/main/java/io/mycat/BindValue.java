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

import io.mycat.util.HexFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * @author mycat
 */
public class BindValue {
    private static final Logger LOGGER = LoggerFactory.getLogger(BindValue.class);
    public boolean isNull; /* NULL indicator */
    public boolean isLongData; /* long data indicator */
    public boolean isSet; /* has this parameter been set */

    public long length; /* Default length of data */
    public int type; /* data type */
    public byte scale;

    /**
     * 数据值
     **/
    public byte byteBinding;
    public short shortBinding;
    public int intBinding;
    public float floatBinding;
    public long longBinding;
    public double doubleBinding;
    public Object value; /* Other value to store */

    public void reset() {
        this.isNull = false;
        this.isLongData = false;
        this.isSet = false;

        this.length = 0;
        this.type = 0;
        this.scale = 0;

        this.byteBinding = 0;
        this.shortBinding = 0;
        this.intBinding = 0;
        this.floatBinding = 0;
        this.longBinding = 0L;
        this.doubleBinding = 0D;
        this.value = null;
    }

    public Object getJavaObject() {
        // 非空情况, 根据字段类型获取值
        switch (type & 0xff) {
            case MysqlDefs.FIELD_TYPE_TINY:
                return byteBinding;
            case MysqlDefs.FIELD_TYPE_SHORT:
                return shortBinding;
            case MysqlDefs.FIELD_TYPE_LONG:
                return intBinding;
            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return longBinding;
            case MysqlDefs.FIELD_TYPE_FLOAT:
                return floatBinding;
            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return doubleBinding;
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
                return value;
            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
            case MysqlDefs.FIELD_TYPE_BLOB:
            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
                byte[] bytes = null;
                if (value instanceof byte[]) {
                    bytes = (byte[]) value;
                }
                if (value instanceof ByteArrayOutputStream) {
                    bytes = ((ByteArrayOutputStream) value).toByteArray();
                }
                if (value instanceof String) {
                    bytes = ((String) value).getBytes();
                }
                if (bytes == null) {
                    throw new UnsupportedOperationException();
                }
                return "X'" + HexFormatUtil.bytesToHexString(bytes) + "'";
            case MysqlDefs.FIELD_TYPE_TIME:
            case MysqlDefs.FIELD_TYPE_DATE:
            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                return value;
            default:
                return value;
        }
    }
}