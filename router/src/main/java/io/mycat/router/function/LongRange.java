package io.mycat.router.function;

import io.mycat.util.NumberParseUtil;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

final class LongRange {

  public final int nodeIndx;
  public final long valueStart;
  public final long valueEnd;

  public LongRange(int nodeIndx, long valueStart, long valueEnd) {
    super();
    this.nodeIndx = nodeIndx;
    this.valueStart = valueStart;
    this.valueEnd = valueEnd;
  }

  public static int getPartitionCount(LongRange[] ranges) {
    return (int) Stream.of(ranges).mapToInt(i -> i.nodeIndx).distinct().count();
  }

  public static LongRange[] getLongRanges(Map<String, String> ranges) {
    ArrayList<LongRange> longRangeList = new ArrayList<>();
    for (Entry<String, String> entry : ranges.entrySet()) {
      String[] pair = entry.getKey().split("-");
      long longStart = NumberParseUtil.parseLong(pair[0].trim());
      long longEnd = NumberParseUtil.parseLong(pair[1].trim());
      int nodeId = Integer.parseInt(entry.getValue().trim());
      longRangeList.add(new LongRange(nodeId, longStart, longEnd));
    }
    return longRangeList.toArray(new LongRange[longRangeList.size()]);
  }
}