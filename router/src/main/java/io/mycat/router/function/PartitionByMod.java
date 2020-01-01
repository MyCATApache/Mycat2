package io.mycat.router.function;

import io.mycat.router.RuleFunction;

import java.math.BigInteger;
import java.util.Map;
import java.util.Objects;

public class PartitionByMod extends RuleFunction {

  private BigInteger count;

  @Override
  public String name() {
    return "PartitionByMod";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    String count = prot.get("count");
    Objects.requireNonNull(count);
    this.count = new BigInteger(count);
  }

  @Override
  public int calculate(String columnValue) {
    try {
      BigInteger bigNum = new BigInteger(columnValue).abs();
      return (bigNum.mod(count)).intValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
          e);
    }
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return count.intValue();
  }
}