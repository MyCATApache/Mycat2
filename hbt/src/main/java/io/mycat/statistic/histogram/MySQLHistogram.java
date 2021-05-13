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

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Getter
@ToString
public class MySQLHistogram {
    private final LocalDateTime lastUpdated;
    private final boolean equiHeight;
    private final List<MySQLBucket> mySQLBuckets;

    public MySQLHistogram(LocalDateTime lastUpdated, boolean equiHeight, List<MySQLBucket> mySQLBuckets) {
        this.lastUpdated = lastUpdated;
        this.equiHeight = equiHeight;
        this.mySQLBuckets = Collections.unmodifiableList(mySQLBuckets);
    }
}
