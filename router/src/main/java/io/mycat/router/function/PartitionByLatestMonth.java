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

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Map;

public class PartitionByLatestMonth extends RuleFunction {

  private int splitOneDay;
  private int hourSpan;
  private DateTimeFormatter formatter;

  @Override
  public String name() {
    return "PartitionByLatestMonth";
  }

  @Override
  public int calculate(String columnValue) {
    TemporalAccessor date = this.formatter.parse(columnValue);
    int day = date.get(ChronoField.DAY_OF_YEAR);
    int hour = date.get(ChronoField.HOUR_OF_DAY);
    return (day - 1) * splitOneDay + hour / hourSpan;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return -1;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.formatter = DateTimeFormatter.ofPattern(prot.get("dateFormat"));
    this.splitOneDay = Integer.parseInt(prot.get("splitOneDay"));
    hourSpan = 24 / splitOneDay;
    if (hourSpan * 24 < 24) {
      throw new java.lang.IllegalArgumentException(
          "invalid splitOnDay param:"
              + splitOneDay
              + " should be an even number and less or equals than 24");
    }
  }
}