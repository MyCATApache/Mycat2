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

import io.mycat.router.ShardingTableHandler;
import io.mycat.router.SingleValueRuleFunction;

import java.math.BigInteger;
import java.util.Map;

public class PartitionByHashMod extends SingleValueRuleFunction {

  private int count;
  private boolean watch;

  @Override
  public String name() {
    return "PartitionByHashMod";
  }

  @Override
  public int calculateIndex(String columnValue) {
    BigInteger bigNum = BigInteger.valueOf(hash(columnValue.hashCode())).abs();
    if (watch) {
      return bigNum.intValue() & (count - 1);
    }
    return (bigNum.mod(BigInteger.valueOf(count))).intValue();
  }

  @Override
  public int[] calculateIndexRange(String beginValue, String endValue) {
    return null;
  }


  @Override
  public void init(ShardingTableHandler tableHandler,Map<String, String> prot, Map<String, String> ranges) {
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