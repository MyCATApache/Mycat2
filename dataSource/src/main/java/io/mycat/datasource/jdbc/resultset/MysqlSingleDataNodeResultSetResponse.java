/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc.resultset;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.proxy.MySQLPacketUtil;

import java.io.IOException;
import java.util.Iterator;
/**
 * @author Junwen Chen
 **/
public class MysqlSingleDataNodeResultSetResponse implements MycatResultSetResponse {

  final RowBaseIterator rowBaseIterator;

  public MysqlSingleDataNodeResultSetResponse(RowBaseIterator rowBaseIterator) {
    this.rowBaseIterator = rowBaseIterator;
  }

  @Override
  public int columnCount() {
    return rowBaseIterator.metaData().getColumnCount();
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return new Iterator<byte[]>() {
      final int count = MysqlSingleDataNodeResultSetResponse.this.columnCount();
      int index = 1;

      @Override
      public boolean hasNext() {
        return index <= count;
      }

      @Override
      public byte[] next() {
        return MySQLPacketUtil
            .generateColumnDefPayload(
                MysqlSingleDataNodeResultSetResponse.this.rowBaseIterator.metaData(),
                index++);
      }
    };
  }

  @Override
  public Iterator<byte[]> rowIterator() {
    return new Iterator<byte[]>() {
      final int count = MysqlSingleDataNodeResultSetResponse.this.columnCount();

      @Override
      public boolean hasNext() {
        return rowBaseIterator.next();
      }

      @Override
      public byte[] next() {
        //todo optimize to remove tmp array
        RowBaseIterator rowBaseIterator = MysqlSingleDataNodeResultSetResponse.this.rowBaseIterator;
        byte[][] bytes = new byte[count][];
        for (int i = 0, j = 1; i < count; i++, j++) {
          bytes[i] = rowBaseIterator.getBytes(j);
        }
        return MySQLPacketUtil.generateTextRow(bytes);
      }
    };
  }

  @Override
  public void close() throws IOException {
    rowBaseIterator.close();
  }
}