package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import io.mycat.router.util.PartitionUtil;
import io.mycat.util.Pair;
import io.mycat.util.StringUtil;
import java.util.Map;

public class PartitionByString extends RuleAlgorithm {

  private int hashSliceStart;
  private int hashSliceEnd;
  private PartitionUtil partitionUtil;

  /**
   * "2" -&gt; (0,2)<br/> "1:2" -&gt; (1,2)<br/> "1:" -&gt; (1,0)<br/> "-1:" -&gt; (-1,0)<br/> ":-1"
   * -&gt; (0,-1)<br/> ":" -&gt; (0,0)<br/>
   */
  public static Pair<Integer, Integer> sequenceSlicing(String slice) {
    int ind = slice.indexOf(':');
    if (ind < 0) {
      int i = Integer.parseInt(slice.trim());
      if (i >= 0) {
        return new Pair<>(0, i);
      } else {
        return new Pair<>(i, 0);
      }
    }
    String left = slice.substring(0, ind).trim();
    String right = slice.substring(1 + ind).trim();
    int start, end;
    if (left.length() <= 0) {
      start = 0;
    } else {
      start = Integer.parseInt(left);
    }
    if (right.length() <= 0) {
      end = 0;
    } else {
      end = Integer.parseInt(right);
    }
    return new Pair<>(start, end);
  }

  @Override
  public String name() {
    return "PartitionByString";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    String partitionLengthText = prot.get("partitionLength");
    String partitionCountText = prot.get("partitionCount");
    String hashSliceText = prot.get("hashSlice");
    Pair<Integer, Integer> pair = sequenceSlicing(hashSliceText);
    this.hashSliceStart = pair.getKey();
    this.hashSliceEnd = pair.getValue();

    partitionUtil = new PartitionUtil(toIntArray(partitionCountText),
        toIntArray(partitionLengthText));

  }

  @Override
  public int calculate(String columnValue) {
    int start = hashSliceStart >= 0 ? hashSliceStart : columnValue.length() + hashSliceStart;
    int end = hashSliceEnd > 0 ? hashSliceEnd : columnValue.length() + hashSliceEnd;
    long hash = StringUtil.hash(columnValue, start, end);
    return partitionUtil.partition(hash);
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return partitionUtil.getPartitionNum();
  }


}