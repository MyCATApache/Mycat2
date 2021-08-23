/**
 * Copyright (C) <2021>  <mycat>
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

import io.mycat.router.CustomRuleFunction;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Objects;

public class PartitionByLatestMonth extends Mycat1xSingleValueRuleFunction {

    private int splitOneDay;
    private int hourSpan;
    private DateTimeFormatter formatter;

    @Override
    public String name() {
        return "PartitionByLatestMonth";
    }

    @Override
    public int calculateIndex(String columnValue) {
        TemporalAccessor date = this.formatter.parse(columnValue);
        int day = date.get(ChronoField.DAY_OF_YEAR);
        int hour = date.get(ChronoField.HOUR_OF_DAY);
        return (day - 1) * splitOneDay + hour / hourSpan;
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return calculateSequenceRange(this, beginValue, endValue);
    }

    @Override
    public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
        this.formatter = DateTimeFormatter.ofPattern(Objects.toString(prot.get("dateFormat")));
        this.splitOneDay = Integer.parseInt(Objects.toString(prot.get("splitOneDay")));
        hourSpan = 24 / splitOneDay;
        if (hourSpan * 24 < 24) {
            throw new java.lang.IllegalArgumentException(
                    "invalid splitOnDay param:"
                            + splitOneDay
                            + " should be an even number and less or equals than 24");
        }
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionByLatestMonth.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByLatestMonth ruleFunction = (PartitionByLatestMonth) customRuleFunction;
            int splitOneDay = ruleFunction.splitOneDay;
            int hourSpan = ruleFunction.hourSpan;
            DateTimeFormatter formatter = ruleFunction.formatter;
            return this.splitOneDay == splitOneDay && this.hourSpan == hourSpan && Objects.equals(this.formatter, formatter);
        }
        return false;
    }
    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+ splitOneDay+hourSpan+formatter;
    }
}