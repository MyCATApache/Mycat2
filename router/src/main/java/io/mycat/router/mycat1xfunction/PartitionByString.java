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
package io.mycat.router.mycat1xfunction;

import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.util.PartitionUtil;
import io.mycat.util.Pair;
import io.mycat.util.StringUtil;

import java.util.Map;
import java.util.Objects;

public class PartitionByString extends Mycat1xSingleValueRuleFunction {

  private int hashSliceStart;
  private int hashSliceEnd;
  private PartitionUtil partitionUtil;


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
  public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
    String partitionLengthText = Objects.toString(prot.get("partitionLength"));
    String partitionCountText = Objects.toString(prot.get("partitionCount"));
    String hashSliceText = Objects.toString(prot.get("hashSlice"));
    Pair<Integer, Integer> pair = sequenceSlicing(hashSliceText);
    this.hashSliceStart = pair.getKey();
    this.hashSliceEnd = pair.getValue();

    partitionUtil = new PartitionUtil(toIntArray(partitionCountText),
        toIntArray(partitionLengthText));

  }

  @Override
  public int calculateIndex(String columnValue) {
    int start = hashSliceStart >= 0 ? hashSliceStart : columnValue.length() + hashSliceStart;
    int end = hashSliceEnd > 0 ? hashSliceEnd : columnValue.length() + hashSliceEnd;
    long hash = StringUtil.hash(columnValue, start, end);
    return partitionUtil.partition(hash);
  }

  @Override
  public int[] calculateIndexRange(String beginValue, String endValue) {
    return null;
  }




}