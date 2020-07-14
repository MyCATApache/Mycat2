/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt3;

import io.mycat.hbt4.ShardingInfo;

import java.util.stream.IntStream;


public class RangePartInfo implements PartInfo {
    private final int startIndex;
    private final int endIndex;
    private final ShardingInfo shardingInfo;


    public RangePartInfo(int startIndex, int endIndex, ShardingInfo shardingInfo) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.shardingInfo = shardingInfo;
    }

    @Override
    public int size() {
        return endIndex - startIndex;
    }

    @Override
    public Part getPart(int index) {
        int schemaSize = shardingInfo.getSchemaSize();
        int tableSize = shardingInfo.getTableSize();

        int count = 0;
        for (int i = 0; i < schemaSize; i++) {
            for (int j = 0; j < tableSize; j++) {
                if (count == index) {
                    return new PartImpl(shardingInfo.getDatasourceSize(), i, j);
                }
                count++;
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Part[] toPartArray() {
        return IntStream.range(startIndex, endIndex).mapToObj(i -> getPart(i)).toArray(i -> new Part[i]);
    }

    @Override
    public String toString() {
        return "[" +
                startIndex + "-" + endIndex + "]";
    }
}