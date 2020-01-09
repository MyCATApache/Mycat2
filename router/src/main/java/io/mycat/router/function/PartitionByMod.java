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