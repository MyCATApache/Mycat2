/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.api.collector;

import io.mycat.beans.mysql.packet.ColumnDefPacket;

import java.math.BigDecimal;

/**
 * @author jamie12221
 *  date 2019-05-11 14:44 文本结果集收集类
 **/

public interface ResultSetCollector {

    void onResultSetStart();

    void onResultSetEnd();

    void onRowStart();

    void onRowEnd();

    void addNull(int columnIndex);

    void addString(int columnIndex, String value);

    void addBlob(int columnIndex, byte[] value);


    void addValue(int columnIndex, long value, boolean isNUll);

    void addValue(int columnIndex, double value, boolean isNUll);

    void addValue(int columnIndex, byte value, boolean isNUll);


    void addDecimal(int columnIndex, BigDecimal value);

    void collectColumnList(ColumnDefPacket[] packets);

    void addDate(int columnIndex, java.util.Date date);
}
