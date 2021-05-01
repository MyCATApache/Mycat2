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
package io.mycat.resultset;

import io.mycat.MySQLPacketUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;

import java.sql.Types;
import java.util.Iterator;

/**
 * @author Junwen Chen
 **/
public class DirectTextResultSetResponse extends AbstractMycatResultSetResponse {

    public DirectTextResultSetResponse(RowBaseIterator iterator) {
        super(iterator);
    }

    @Override
    public Iterator<byte[]> rowIterator() {
        final RowBaseIterator rowBaseIterator = iterator;
        final MycatRowMetaData mycatRowMetaData = rowBaseIterator.getMetaData();
        final int columnCount = mycatRowMetaData.getColumnCount();

        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return rowBaseIterator.next();
            }

            @Override
            public byte[] next() {
                byte[][] row = new byte[columnCount][];
                for (int columnIndex = 0, rowIndex = 0; rowIndex < columnCount; columnIndex++, rowIndex++) {

                    int columnType = mycatRowMetaData.getColumnType(columnIndex);
                    switch (columnType){
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                        case Types.BINARY:
                            byte[] bytes = rowBaseIterator.getBytes(columnIndex);
                            row[rowIndex] = rowBaseIterator.wasNull()?null:bytes;
                            break;
                        default:
                            String string = rowBaseIterator.getString(columnIndex);
                            row[rowIndex] = rowBaseIterator.wasNull()?null:string.getBytes();
                            break;
                    }

                }
                return MySQLPacketUtil.generateTextRow(row);
            }
        };
    }
    @Override
    public void close() {
        this.iterator.close();
    }
}