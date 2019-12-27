package io.mycat.router.function;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.mycat.router.RuleFunction;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class PartitionByMurmurHash extends RuleFunction {

  private static final int DEFAULT_WEIGHT = 1;
  private final SortedMap<Integer, Integer> bucketMap = new TreeMap<>();
  private HashFunction hash;
  private int count;

  private static int getWeight(Map<Integer, Integer> weightMap, int bucket) {
    Integer w = weightMap.get(bucket);
    if (w == null) {
      w = DEFAULT_WEIGHT;
    }
    return w;
  }

  @Override
  public String name() {
    return "PartitionByMurmurHash";
  }

  @Override
  public int calculate(String columnValue) {
    SortedMap<Integer, Integer> tail = bucketMap
        .tailMap(hash.hashUnencodedChars(columnValue).asInt());
    if (tail.isEmpty()) {
      return bucketMap.get(bucketMap.firstKey());
    }
    return tail.get(tail.firstKey());
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return count;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    int seed = Integer.parseInt(prot.get("seed"));
    this.count = Integer.parseInt(prot.get("count"));
    int virtualBucketTimes = Integer.parseInt(prot.get("virtualBucketTimes"));
    initBucketMap(ranges, seed, count, virtualBucketTimes);
  }

  private void initBucketMap(Map<String, String> ranges, int seed, int count,
      int virtualBucketTimes) {
    Map<Integer, Integer> weightMap = new HashMap<>();
    for (Entry<String, String> entry : ranges.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      int weight = Integer.parseInt(value);
      weightMap.put(Integer.parseInt(key), weight > 0 ? weight : 1);
    }

    hash = Hashing.murmur3_32(seed);//计算一致性哈希的对象
    for (int i = 0; i < count; i++) {//构造一致性哈希环，用TreeMap表示
      StringBuilder hashName = new StringBuilder("SHARD-").append(i);
      for (int n = 0, shard = virtualBucketTimes * getWeight(weightMap, i); n < shard; n++) {
        bucketMap.put(hash.hashUnencodedChars(hashName.append("-NODE-").append(n)).asInt(), i);
      }
    }
  }
}