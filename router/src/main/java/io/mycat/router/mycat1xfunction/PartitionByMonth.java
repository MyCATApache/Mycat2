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
import io.mycat.util.StringUtil;
import lombok.SneakyThrows;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class PartitionByMonth extends Mycat1xSingleValueRuleFunction {
    private String sBeginDate;
    /**
     * 默认格式
     */
    private String dateFormat = "yyyy-MM-dd";
    /**
     * 场景
     */
    private int scene = -1;
    private String sEndDate;
    private Calendar beginDate;
    private Calendar endDate;
    private int nPartition;

    private ThreadLocal<SimpleDateFormat> formatter;

    @Override
    public String name() {
        return "PartitionByMonth";
    }

    @Override
    public int calculateIndex(String columnValue) {
        try {
            if (scene == 1) {
                Calendar curTime = Calendar.getInstance();
                curTime.setTime(formatter.get().parse(columnValue));
                return curTime.get(Calendar.MONTH);
            }
            int targetPartition;
            Calendar curTime = Calendar.getInstance();
            curTime.setTime(formatter.get().parse(columnValue));
            targetPartition = ((curTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
                    * 12 + curTime.get(Calendar.MONTH)
                    - beginDate.get(Calendar.MONTH));

            /**
             * For circulatory partition, calculated value of target partition needs to be
             * rotated to fit the partition range
             */
            if (nPartition > 0) {
                targetPartition = reCalculatePartition(targetPartition);
            }
            // 防止越界的情况
            if (targetPartition < 0) {
                targetPartition = 0;
            }
            return targetPartition;

        } catch (ParseException e) {
            throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue)
                    .append(" Please check if the format satisfied.").toString(), e);
        }
    }

    @Override
    @SneakyThrows
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return doCalculateRange(beginValue, endValue, beginDate);
    }


    @Override
    public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
        sBeginDate = Objects.toString(prot.get("beginDate"));
        sEndDate = Objects.toString(prot.get("endDate"));
        dateFormat = Objects.toString(prot.get("dateFormat"));

        try {
            if (io.mycat.util.StringUtil.isEmpty(sBeginDate) && StringUtil.isEmpty(sEndDate)) {
                nPartition = 12;
                scene = 1;
                initFormatter();
                beginDate = Calendar.getInstance();
                beginDate.set(Calendar.MONTH, 0);
                endDate = Calendar.getInstance();
                endDate.set(Calendar.MONTH, 11);
                return;
            }
            beginDate = Calendar.getInstance();
            beginDate.setTime(new SimpleDateFormat(dateFormat)
                    .parse(sBeginDate));
            initFormatter();
            if (sEndDate != null && !sEndDate.equals("")) {
                endDate = Calendar.getInstance();
                endDate.setTime(new SimpleDateFormat(dateFormat).parse(sEndDate));
                nPartition = ((endDate.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR)) * 12
                        + endDate.get(Calendar.MONTH) - beginDate.get(Calendar.MONTH)) + 1;

                if (nPartition <= 0) {
                    throw new java.lang.IllegalArgumentException("Incorrect time range for month partitioning!");
                }
            } else {
                nPartition = -1;
            }
        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }

    private int[] doCalculateRange(String beginValue, String endValue, Calendar beginDate) throws ParseException {
        int startPartition, endPartition;
        Calendar partitionTime = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        partitionTime.setTime(format.parse(beginValue));
        startPartition = ((partitionTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
                * 12 + partitionTime.get(Calendar.MONTH)
                - beginDate.get(Calendar.MONTH));
        partitionTime.setTime(format.parse(endValue));
        endPartition = ((partitionTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
                * 12 + partitionTime.get(Calendar.MONTH)
                - beginDate.get(Calendar.MONTH));

        List<Integer> list = new ArrayList<>();

        while (startPartition <= endPartition) {
            Integer nodeValue = reCalculatePartition(startPartition);
            if (nodeValue < 0) {
                nodeValue = 0;
            }
            if (Collections.frequency(list, nodeValue) < 1) {
                list.add(nodeValue);
            }
            startPartition++;
        }
        int size = list.size();
        // 当在场景1： "2015-01-01", "2014-04-03" 范围出现的时候
        // 是应该返回null 还是返回 [] ?
        return list.stream().mapToInt(i -> i).toArray();
    }

    /**
     * For circulatory partition, calculated value of targetName partition needs to be rotated to fit the
     * partition range
     */
    private int reCalculatePartition(int targetPartition) {
        // 没有指定end_date，不是循环使用的情况，直接返回对应的targetPartition
        if (nPartition == -1) {
            return targetPartition;
        }
        /**
         * If target date is previous of start time of partition setting, shift
         * the delta range between target and start date to be positive value
         */
        if (targetPartition < 0) {
            targetPartition = nPartition - (-targetPartition) % nPartition;
        }

        if (targetPartition >= nPartition) {
            targetPartition = targetPartition % nPartition;
        }

        return targetPartition;
    }

    enum Type {
        DEFAULT, UNLIMITED,
    }


    private void initFormatter() {
        formatter = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(dateFormat);
            }
        };
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionByMonth.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByMonth ruleFunction = (PartitionByMonth) customRuleFunction;

            int partition = ruleFunction.nPartition;
            int scene = ruleFunction.scene;
            Object formatter = ruleFunction.formatter;
            Object beginDate = ruleFunction.beginDate;
            Object endDate = ruleFunction.endDate;
            Object sEndDate = ruleFunction.sEndDate;

            return
                            Objects.equals(this.nPartition, partition) &&
                            Objects.equals(this.scene, scene) &&
                            Objects.equals(this.formatter, formatter) &&
                            Objects.equals(this.beginDate, beginDate) &&
                            Objects.equals(this.endDate, endDate) &&
                                    Objects.equals(this.sEndDate, sEndDate);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return "" + nPartition + scene + formatter + beginDate + endDate+sEndDate;
    }
}