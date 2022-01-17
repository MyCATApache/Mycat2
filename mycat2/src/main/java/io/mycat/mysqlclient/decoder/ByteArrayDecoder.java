/**
 * Copyright (C) <2022>  <chen junwen>
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

package io.mycat.mysqlclient.decoder;

import io.mycat.mysqlclient.Decoder;
import io.mycat.vertx.ReadView;
import io.vertx.core.buffer.Buffer;

public class ByteArrayDecoder implements Decoder<byte[][]> {
    int columnCount;

    @Override
    public void initColumnCount(int count) {
        this.columnCount = count;
    }

    @Override
    public void addColumn(int index,Buffer buffer) {

    }

    @Override
    public byte[][] convert(Buffer payload) {
        final int NULL = 0xFB;
        byte[][] row = new byte[columnCount][];
        ReadView readView = new ReadView(payload);
        // TEXT row decoding
        for (int c = 0; c < columnCount; c++) {
            if ((readView.getByte() & 0xff) == NULL) {
                readView.skipInReading(1);
            } else {
                row[c] = readView.readLenencBytes();
            }
        }
        return row;
    }

    @Override
    public void onColumnEnd() {

    }
}
