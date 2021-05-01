/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.querycondition;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
/**
 * @author Junwen Chen
 **/
public class ColumnRangeValue {
    final SQLColumnDefinition column;
    final Object begin;
    final Object end;
    final SQLTableSource tableSource;

    public ColumnRangeValue(SQLColumnDefinition column, Object begin, Object end, SQLTableSource tableSource) {
        this.column = column;
        this.begin = begin;
        this.end = end;
        this.tableSource = tableSource;
    }

    public SQLColumnDefinition getColumn() {
        return column;
    }


    public Object getBegin() {
        return begin;
    }


    public Object getEnd() {
        return end;
    }

    public SQLTableSource getTableSource() {
        return tableSource;
    }
}