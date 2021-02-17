/**
 * Copyright (C) <2020>  <mycat>
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
package io.mycat.router;

import io.mycat.util.NumberParseUtil;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * @author cjw
 */
@Value
@EqualsAndHashCode
public final class NodeIndexRange {
    public final int nodeIndex;
    public final long valueStart;
    public final long valueEnd;

    public NodeIndexRange(int nodeIndex, long valueStart, long valueEnd) {
        super();
        this.nodeIndex = nodeIndex;
        this.valueStart = valueStart;
        this.valueEnd = valueEnd;
    }

    public static int getPartitionCount(List<NodeIndexRange> ranges) {
        return (int) ranges.stream().mapToInt(i -> i.getNodeIndex()).distinct().count();
    }

    public static List<NodeIndexRange> getLongRanges(Map<String, Object> ranges) {
        ArrayList<NodeIndexRange> longRangeList = new ArrayList<>();
        for (Entry<String, Object> entry : ranges.entrySet()) {
            String[] pair = entry.getKey().split("-");
            long longStart = NumberParseUtil.parseLong(pair[0].trim());
            long longEnd = NumberParseUtil.parseLong(pair[1].trim());
            int nodeId = Integer.parseInt(entry.getValue().toString().trim());
            longRangeList.add(new NodeIndexRange(nodeId, longStart, longEnd));
        }
        longRangeList.sort(Comparator.comparing(x -> x.valueStart));
        return longRangeList;
    }

    public static Map<String, String> from(List<List<NodeIndexRange>> lists) {
        lists = lists.stream().sorted(Comparator.comparing(x -> x.get(0).valueStart)).sorted().collect(Collectors.toList());
        return lists.stream().flatMap(k -> k.stream())
                .collect(Collectors.groupingBy(i -> String.valueOf(i.nodeIndex), Collectors.mapping(x -> x.valueStart + "-" + x.valueEnd, Collectors.joining(","))));/**/
    }

    /**
     * @param ranges
     * @return
     */
    public static List<List<NodeIndexRange>> getSplitLongRanges(Map<String, Object> ranges) {
        ArrayList<List<NodeIndexRange>> lists = new ArrayList<>();
        for (Entry<String, Object> entry : ranges.entrySet()) {
            String[] split = entry.getKey().split(",");
            ArrayList<NodeIndexRange> longRangeList = new ArrayList<>();
            for (String s : split) {
                String[] pair = s.split("-");
                long longStart = NumberParseUtil.parseLong(pair[0].trim());
                long longEnd = NumberParseUtil.parseLong(pair[1].trim());
                int nodeId = Integer.parseInt(entry.getValue().toString().trim());
                longRangeList.add(new NodeIndexRange(nodeId, longStart, longEnd));
                longRangeList.sort(Comparator.comparing(x -> x.valueStart));//顺序
            }
            lists.add(longRangeList);
        }
        return lists.stream().sorted(Comparator.comparing(x -> x.get(0).valueStart)).sorted().collect(Collectors.toList());
    }

    public long getSize() {
        return this.valueEnd - this.valueStart + 1;
    }
}