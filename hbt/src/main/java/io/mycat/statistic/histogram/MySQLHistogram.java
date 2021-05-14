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

import io.mycat.util.JsonUtil;
import io.vertx.core.json.Json;
import lombok.Getter;
import lombok.ToString;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

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

    public String toJson() {
        HashMap<Object, Object> root = new HashMap<>();
        root.put("last-updated", Timestamp.valueOf(lastUpdated).toString());
        root.put("histogram-type", equiHeight ? "equi-height" : "singleton");
        List<Object> list = new ArrayList<>();
        if (equiHeight) {
            for (MySQLBucket mySQLBucket : mySQLBuckets) {
                list.add(Objects.toString(mySQLBucket.getLowerInclusiveValue()));
                list.add(Objects.toString(mySQLBucket.getUpperInclusiveValue()));
                list.add(Objects.toString(mySQLBucket.getCumulativeFrequence()));
                list.add(Objects.toString(mySQLBucket.getNumberOfDistinctValues()));
            }
        } else {
            for (MySQLBucket mySQLBucket : mySQLBuckets) {
                list.add(Objects.toString(mySQLBucket.getLowerInclusiveValue()));
                list.add(Objects.toString(mySQLBucket.getCumulativeFrequence()));
            }
        }
        root.put("buckets", list);
        return JsonUtil.toJson(root);
    }
}
