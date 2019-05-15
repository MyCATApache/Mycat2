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
package io.mycat.proxy.task.client.resultset;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import java.util.ArrayList;

/**
 * @author jamie12221
 * @date 2019-05-15 21:57 文本多结果集收集器
 **/
public class MultiQueryResultSetCollector extends QueryResultSetCollector {

  final ArrayList[][] mresult;
  final ColumnDefPacket[][] mcolumns;
  private int resultSetIndex = 0;

  public MultiQueryResultSetCollector(int count) {
    this.mresult = new ArrayList[count][];
    this.mcolumns = new ColumnDefPacket[count][];
  }

  @Override
  public void onRowEnd() {
    mcolumns[resultSetIndex] = super.columns;
    mresult[resultSetIndex] = super.result;
    super.columns = null;
    super.result = null;
    resultSetIndex++;
  }
}
