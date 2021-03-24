/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertxmycat;

import io.netty.buffer.ByteBuf;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.MySQLRowImpl;
import io.vertx.mysqlclient.impl.datatype.DataFormat;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.datatype.DataTypeCodec;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.RowDecoder;

import java.util.stream.Collector;

public class MycatVertxRowResultDecoder<C, R> extends RowDecoder<C, R> {
    private static final int NULL = 0xFB;

    MySQLRowDesc rowDesc;

    MycatVertxRowResultDecoder(Collector<Row, C, R> collector, MySQLRowDesc rowDesc) {
        super(collector);
        this.rowDesc = rowDesc;
    }

    @Override
    protected Row decodeRow(int len, ByteBuf in) {
        Row row = new MySQLRowImpl(rowDesc);
        if (rowDesc.dataFormat() == DataFormat.BINARY) {
            // BINARY row decoding
            // 0x00 packet header
            // null_bitmap
            int nullBitmapLength = (len + 7 + 2) >> 3;
            int nullBitmapIdx = 1 + in.readerIndex();
            in.skipBytes(1 + nullBitmapLength);

            // values
            for (int c = 0; c < len; c++) {
                int val = c + 2;
                int bytePos = val >> 3;
                int bitPos = val & 7;
                byte mask = (byte) (1 << bitPos);
                byte nullByte = (byte) (in.getByte(nullBitmapIdx + bytePos) & mask);
                Object decoded = null;
                if (nullByte == 0) {
                    // non-null
                    ColumnDefinition columnDef = rowDesc.columnDefinitions()[c];
                    DataType dataType = columnDef.type();
                    int collationId = rowDesc.columnDefinitions()[c].characterSet();
                    int columnDefinitionFlags = columnDef.flags();
                    decoded = DataTypeCodec.decodeBinary(dataType, collationId, columnDefinitionFlags, in);
                }
                row.addValue(decoded);
            }
        } else {
            // TEXT row decoding
            for (int c = 0; c < len; c++) {
                Object decoded = null;
                if (in.getUnsignedByte(in.readerIndex()) == NULL) {
                    in.skipBytes(1);
                } else {
                    DataType dataType = rowDesc.columnDefinitions()[c].type();
                    int columnDefinitionFlags = rowDesc.columnDefinitions()[c].flags();
                    int collationId = rowDesc.columnDefinitions()[c].characterSet();
                    decoded = DataTypeCodec.decodeText(dataType, collationId, columnDefinitionFlags, in);
                }
                row.addValue(decoded);
            }
        }
        return row;
    }
}

