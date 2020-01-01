package io.mycat.router.function;

import io.mycat.util.NumberParseUtil;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

public class GroupSizeRange {

  public final int groupSize;
  public final long valueStart;
  public final long valueEnd;

  public GroupSizeRange(int groupSize, long valueStart, long valueEnd) {
    this.groupSize = groupSize;
    this.valueStart = valueStart;
    this.valueEnd = valueEnd;
  }

  public static int getPartitionCount(GroupSizeRange[] longRanges) {
    return (int) Stream.of(longRanges).mapToInt(i -> i.groupSize).distinct().count();

  }

  public static GroupSizeRange[] getGroupSizeRange(Map<String, String> ranges) {
    ArrayList<GroupSizeRange> longRangeList = new ArrayList<>();
    for (Entry<String, String> entry : ranges.entrySet()) {
      String[] pair = entry.getKey().split("-");
      long longStart = NumberParseUtil.parseLong(pair[0].trim());
      long longEnd = NumberParseUtil.parseLong(pair[1].trim());
      int nodeId = Integer.parseInt(entry.getValue().trim());
      longRangeList.add(new GroupSizeRange(nodeId, longStart, longEnd));
    }
    return longRangeList.toArray(new GroupSizeRange[longRangeList.size()]);
  }
}