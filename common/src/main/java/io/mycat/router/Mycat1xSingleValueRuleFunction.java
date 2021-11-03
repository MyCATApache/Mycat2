/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.router;

import com.alibaba.druid.sql.SQLUtils;
import io.mycat.Partition;
import io.mycat.MycatException;
import io.mycat.RangeVariable;
import io.mycat.ShardingTableType;
import io.mycat.util.CollectionUtil;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author mycat
 * @author cjw
 * 路由算法接口
 */
public abstract class Mycat1xSingleValueRuleFunction extends CustomRuleFunction {

    private String columnName;
    private ShardingTableType shardingTableType;

    public static int[] toIntArray(String string) {
        String[] strs = io.mycat.util.SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    /**
     * 对于存储数据按顺序存放的字段做范围路由，可以使用这个函数
     */
    public static int[] calculateSequenceRange(Mycat1xSingleValueRuleFunction algorithm, String beginValue,
                                               String endValue) {
        int begin = 0, end = 0;
        begin = algorithm.calculateIndex(beginValue);
        end = algorithm.calculateIndex(endValue);
        if (end >= begin) {
            int len = end - begin + 1;
            int[] re = new int[len];
            for (int i = 0; i < len; i++) {
                re[i] = begin + i;
            }
            return re;
        } else {
            return new int[0];
        }
    }

    public static int[] calculateAllRange(int count) {
        int[] ints = new int[count];
        for (int i = 0; i < count; i++) {
            ints[i] = i;
        }
        return ints;
    }

    protected static int[] ints(List<Integer> list) {
        int[] ints = new int[list.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = list.get(i);
        }
        return ints;
    }

    public abstract String name();

    @Override
    public synchronized void callInit(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {
        super.callInit(tableHandler, properties, ranges);
        this.columnName = Objects.requireNonNull(
                properties.get("columnName"), "need columnName").toString();

    }

    @Override
    public List<Partition> calculate(Map<String, RangeVariable> values) {
        ArrayList<Partition> res = new ArrayList<>();
        for (RangeVariable rangeVariable : values.values()) {
            //匹配字段名
            if (getColumnName().equalsIgnoreCase(rangeVariable.getColumnName())) {
                ///////////////////////////////////////////////////////////////
                String begin = Objects.toString(rangeVariable.getBegin());
                String end = Objects.toString(rangeVariable.getEnd());
                switch (rangeVariable.getOperator()) {
                    case EQUAL: {
                        Partition partition = this.calculate(begin);
                        if (partition != null) {
                            CollectionUtil.setOpAdd(res, partition);
                        } else {
                            return getTable().dataNodes();
                        }
                        break;
                    }
                    case RANGE: {
                        List<Partition> partitions = this.calculateRange(begin, end);
                        if (partitions == null || partitions.size() == 0) {
                            return getTable().dataNodes();
                        }
                        CollectionUtil.setOpAdd(res, partitions);
                        break;
                    }
                }
            }
        }
        return res.isEmpty() ? getTable().dataNodes() : res;
    }

    public String getColumnName() {
        return columnName;
    }

    /**
     * return matadata nodes's id columnValue is column's value
     *
     * @return never null
     */
    public abstract int calculateIndex(String columnValue);

    public abstract int[] calculateIndexRange(String beginValue, String endValue);

    public Partition calculate(String columnValue) {
        int i = calculateIndex(columnValue);
        if (i == -1) {
            return null;
        }
        ShardingTableHandler table = getTable();
        List<Partition> shardingBackends = table.dataNodes();
        int size = shardingBackends.size();
        if (0 <= i && i < size) {
            return shardingBackends.get(i);
        } else {
            String message = MessageFormat.format("{0}.{1} 分片算法越界 分片值:{4}",
                    table.getSchemaName(), table.getTableName(), columnValue);
            throw new MycatException(message);
        }
    }


    public List<Partition> calculateRange(String beginValue, String endValue) {
        int[] ints = calculateIndexRange(beginValue, endValue);
        ShardingTableHandler table = getTable();
        List<Partition> shardingBackends = (List) table.dataNodes();
        int size = shardingBackends.size();
        if (ints == null) {
            return shardingBackends;
        }
        ArrayList<Partition> res = new ArrayList<>();
        for (int i : ints) {
            if (0 <= i && i < size) {
                res.add(shardingBackends.get(i));
            } else {
                return shardingBackends;
            }
        }
        return res;
    }

    @Override
    public boolean isShardingKey(String name) {
        return isShardingTableKey(SQLUtils.normalize(name));
    }

    @Override
    public boolean isShardingDbKey(String name) {
        return false;
    }

    @Override
    public boolean isShardingTableKey(String name) {
        return this.columnName.equalsIgnoreCase(name);
    }

    @Override
    public ShardingTableType getShardingTableType() {
        if (this.shardingTableType == null) {
            this.shardingTableType = ShardingTableType.compute(this.calculate(Collections.emptyMap()));
        }
        return shardingTableType;
    }
}