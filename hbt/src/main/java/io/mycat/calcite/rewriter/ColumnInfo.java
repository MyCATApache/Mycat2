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
package io.mycat.calcite.rewriter;

import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.core.TableScan;

import java.util.Objects;

@Getter
@ToString
public class ColumnInfo {
    private TableScan tableScan;
    private int index;

    public ColumnInfo(TableScan tableScan, int index) {
        this.tableScan = tableScan;
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnInfo that = (ColumnInfo) o;
        return index == that.index && Objects.equals(tableScan, that.tableScan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableScan, index);
    }
}