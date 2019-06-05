package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import java.math.BigInteger;
import java.util.Map;

public class PartitionByHashMod extends RuleAlgorithm {

  private int count;
  private boolean watch;

  @Override
  public String name() {
    return "PartitionByHashMod";
  }

  @Override
  public int calculate(String columnValue) {
    BigInteger bigNum = BigInteger.valueOf(hash(columnValue.hashCode())).abs();
    if (watch) {
      return bigNum.intValue() & (count - 1);
    }
    return (bigNum.mod(BigInteger.valueOf(count))).intValue();
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
    this.watch = false;
    this.count = Integer.parseInt(prot.get("count"));

    if ((count & (count - 1)) == 0) {
      watch = true;
    }
  }

  /**
   * Using Wang/Jenkins Hash
   *
   * @return hash value
   */
  protected int hash(int key) {
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
  }

}