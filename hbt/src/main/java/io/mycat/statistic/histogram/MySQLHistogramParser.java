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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

public class MySQLHistogramParser {
    public static MySQLHistogram parse(String text) {
        Map from = JsonUtil.from(text, Map.class);
        LocalDateTime lastUpdated = Timestamp.valueOf((String) from.get("last-updated")).toLocalDateTime();
        List<List> buckets = Optional.ofNullable((List) from.get("buckets")).orElse(Collections.emptyList());
        boolean equiHeight = ("equi-height".equalsIgnoreCase(Objects.toString(from.get("histogram-type"))));
        List<MySQLBucket> mySQLBuckets = new ArrayList<>(buckets.size());
        if (equiHeight){
            for (List bucket : buckets) {
                Object lowerInclusiveValue = bucket.get(0);
                Object upperInclusiveValue = bucket.get(1);
                Double cumulativeFrequence =  Double.parseDouble( Objects.toString(bucket.get(2)));
                Double numberOfDistinctValues =Double.parseDouble( Objects.toString(bucket.get(2)));

                mySQLBuckets.add( new MySQLBucket(lowerInclusiveValue,upperInclusiveValue,cumulativeFrequence,numberOfDistinctValues));
            }
        }else {
            for (List bucket : buckets) {
                Object value = bucket.get(0);
                Double cumulativeFrequence = Double.parseDouble( Objects.toString(bucket.get(1)));

                mySQLBuckets.add( new MySQLBucket(value,cumulativeFrequence));
            }
        }
        return new MySQLHistogram(lastUpdated,equiHeight,mySQLBuckets);
    }


    public static void main(String[] args) {
        MySQLHistogram  histogram = MySQLHistogramParser.parse("{\n" +
                "  // Last time the histogram was updated. As of now, this means \"when the\n" +
                "  // histogram was created\" (incremental updates are not supported). Date/time\n" +
                "  // is given in UTC.\n" +
                "  // -- J_DATETIME\n" +
                "  \"last-updated\": \"2015-11-04 15:19:51.000000\",\n" +
                "\n" +
                "  // Histogram type. Always \"equi-height\" for equi-height histograms.\n" +
                "  // -- J_STRING\n" +
                "  \"histogram-type\": \"equi-height\",\n" +
                "\n" +
                "  // Histogram buckets. This will always be at least one bucket.\n" +
                "  // -- J_ARRAY\n" +
                "  \"buckets\":\n" +
                "  [\n" +
                "    [\n" +
                "      // Lower inclusive value.\n" +
                "      // -- Data type depends on the source column.\n" +
                "      \"0\",\n" +
                "\n" +
                "      // Upper inclusive value.\n" +
                "      // -- Data type depends on the source column.\n" +
                "      \"002a38227ecc7f0d952e85ffe37832d3f58910da\",\n" +
                "\n" +
                "      // Cumulative frequence\n" +
                "      // -- J_DOUBLE\n" +
                "      0.001978728666831561,\n" +
                "\n" +
                "      // Number of distinct values in this bucket.\n" +
                "      // -- J_UINT\n" +
                "      10\n" +
                "    ]\n" +
                "  ]\n" +
                "}");
        String s = histogram.toString();

        System.out.println();
    }
}
