/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetType;

import java.io.IOException;
import java.util.function.Supplier;

public class RowIterable implements Supplier<RowBaseIterator>, MycatResponse {
    protected RowBaseIterator rowBaseIterator;

    public RowIterable(RowBaseIterator rowBaseIterator) {
        this.rowBaseIterator = rowBaseIterator;
    }

    public RowIterable() {

    }

    public static RowIterable create(RowBaseIterator rowBaseIterator) {
        return new RowIterable(rowBaseIterator);
    }

    @Override
    public MycatResultSetType getType() {
        return MycatResultSetType.RRESULTSET;
    }

    @Override
    public void close() throws IOException {
        if (rowBaseIterator != null) {
            rowBaseIterator.close();
        }
    }

    @Override
    public RowBaseIterator get() {
        return rowBaseIterator;
    }
}