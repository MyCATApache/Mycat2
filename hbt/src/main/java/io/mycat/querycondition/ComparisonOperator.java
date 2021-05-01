/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package io.mycat.querycondition;

import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Comparison operator.
 *
 * @author xu.zx
 */
public enum ComparisonOperator {

  /* greater than */
  GT(">"),
  /* greater than or equal */
  GTE(">="),
  /* less than */
  LT("<"),
  /* less than or equal */
  LTE("<="),
  /* nop */
  NOP("nop");

  String value;

  ComparisonOperator(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static boolean isLowerBoundOp(ComparisonOperator operator) {
    return operator == GT || operator == GTE;
  }

  public static boolean isUpperBoundOp(ComparisonOperator operator) {
    return operator == LT || operator == LTE;
  }

  public static boolean containsEq(ComparisonOperator operator) {
    return operator.value.contains("=");
  }

  public static boolean isLowerBoundOp(String... op) {
    if (op == null) {
      return false;
    }
    return isLowerBoundOp(Arrays.asList(op));
  }

  public static boolean isUpperBoundOp(String... op) {
    if (op == null) {
      return false;
    }
    return isUpperBoundOp(Arrays.asList(op));
  }

  public static boolean isLowerBoundOp(List<String> op) {
    if (op.isEmpty()) {
      return false;
    }
    return op.stream().allMatch(ComparisonOperator::isLowerBoundOp);
  }

  public static boolean isUpperBoundOp(List<String> op) {
    if (op.isEmpty()) {
      return false;
    }
    return op.stream().allMatch(ComparisonOperator::isUpperBoundOp);
  }

  public static boolean isLowerBoundOp(String op) {
    return op.contains(">");
  }

  public static boolean isUpperBoundOp(String op) {
    return op.contains("<");
  }

  // ---------- template method ---------- //

  private static Map<String, ComparisonOperator> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (ComparisonOperator operator : values()) {
      KVS.put(operator.value(), operator);
    }
  }

  public static ComparisonOperator parse(String op) {
    return KVS.get(op);
  }

}
