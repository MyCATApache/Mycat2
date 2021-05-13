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
package io.mycat.statistic.histogram;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class MySQLBucket {
    final Object lowerInclusiveValue;
    final Object upperInclusiveValue;
    final Double cumulativeFrequence;
    final Double numberOfDistinctValues;

    public MySQLBucket(Object lowerInclusiveValue, Object upperInclusiveValue, Double cumulativeFrequence, Double numberOfDistinctValues) {
        this.lowerInclusiveValue = lowerInclusiveValue;
        this.upperInclusiveValue = upperInclusiveValue;
        this.cumulativeFrequence = cumulativeFrequence;
        this.numberOfDistinctValues = numberOfDistinctValues;
    }

    public MySQLBucket(Object value, Double cumulativeFrequence) {
        this.lowerInclusiveValue = value;
        this.cumulativeFrequence = cumulativeFrequence;
        this.upperInclusiveValue = null;
        this.numberOfDistinctValues = null;
    }
}
