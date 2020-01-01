package io.mycat.router.function;

import io.mycat.util.NumberParseUtil;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

final class NodeIndexRange {

  public final int nodeIndex;
  public final long valueStart;
  public final long valueEnd;

  public NodeIndexRange(int nodeIndex, long valueStart, long valueEnd) {
    super();
    this.nodeIndex = nodeIndex;
    this.valueStart = valueStart;
    this.valueEnd = valueEnd;
  }

  public static int getPartitionCount(NodeIndexRange[] ranges) {
    return (int) Stream.of(ranges).mapToInt(i -> i.nodeIndex).distinct().count();
  }

  public static NodeIndexRange[] getLongRanges(Map<String, String> ranges) {
    ArrayList<NodeIndexRange> longRangeList = new ArrayList<>();
    for (Entry<String, String> entry : ranges.entrySet()) {
      String[] pair = entry.getKey().split("-");
      long longStart = NumberParseUtil.parseLong(pair[0].trim());
      long longEnd = NumberParseUtil.parseLong(pair[1].trim());
      int nodeId = Integer.parseInt(entry.getValue().trim());
      longRangeList.add(new NodeIndexRange(nodeId, longStart, longEnd));
    }
    return longRangeList.toArray(new NodeIndexRange[longRangeList.size()]);
  }
}