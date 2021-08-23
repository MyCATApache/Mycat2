package io.mycat.router.mycat1xfunction;

import io.mycat.Partition;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.StringUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 根据日期查询日志数据 冷热数据分布 ，最近n个月的到实时交易库查询，超过n个月的按照m天分片
 *
 * @author sw
 *
 * <tableRule name="sharding-by-date">
 * <rule>
 * <columns>create_time</columns>
 * <algorithm>sharding-by-hotdate</algorithm>
 * </rule>
 * </tableRule>
 * <function name="sharding-by-hotdate" class="org.opencloudb.route.function.PartitionByHotDate">
 * <property name="dateFormat">yyyy-MM-dd</property>
 * <property name="sLastDay">10</property>
 * <property name="sPartionDay">30</property>
 * </function>
 */
public class PartitionByHotDate extends Mycat1xSingleValueRuleFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByHotDate.class);

    private String dateFormat;
    private String sLastDay;
    private String sPartionDay;

    private long sLastTime;
    private long partionTime;
    private ThreadLocal<SimpleDateFormat> formatter;

    private long beginDate;

    private static final long oneDay = 86400000;
    private ShardingTableHandler tableHandler;
    private Map<String, Object> properties;
    private Map<String, Object> ranges;


    @Override
    public String name() {
        return "PartitionByHotDate";
    }

    @Override
    public int calculateIndex(String columnValue) {
        Integer targetPartition = -1;
        try {
            long targetTime = formatter.get().parse(columnValue).getTime();
            Calendar now = Calendar.getInstance();
            long nowTime = now.getTimeInMillis();

            beginDate = nowTime - sLastTime * oneDay;

            long diffDays = (nowTime - targetTime) / (1000 * 60 * 60 * 24) + 1;
            if (diffDays - sLastTime <= 0 || diffDays < 0) {
                targetPartition = 0;
            } else {
                targetPartition = (int) ((beginDate - targetTime) / partionTime) + 1;
            }

            LOGGER.debug("PartitionByHotDate calculate for " + columnValue + " return " + targetPartition);
            return targetPartition;
        } catch (ParseException e) {
            throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(), e);
        }
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        Integer[] integers = calculateIndexRange0(beginValue, endValue);
        return Arrays.stream(integers).mapToInt(i->i).toArray();
    }

    public Integer[] calculateIndexRange0(String beginValue, String endValue) {
        Integer[] targetPartition = null;
        try {
            long startTime = formatter.get().parse(beginValue).getTime();
            long endTime = formatter.get().parse(endValue).getTime();
            Calendar now = Calendar.getInstance();
            long nowTime = now.getTimeInMillis();

            long limitDate = nowTime - sLastTime * oneDay;
            long diffDays = (nowTime - startTime) / (1000 * 60 * 60 * 24) + 1;
            if (diffDays - sLastTime <= 0 || diffDays < 0) {
                Integer[] re = new Integer[1];
                re[0] = 0;
                targetPartition = re;
            } else {
                Integer[] re = null;
                Integer begin = 0, end = 0;
                end = this.calculateIndex(StringUtil.removeBackquote(beginValue));
                boolean hasLimit = false;
                if (endTime - limitDate > 0) {
                    endTime = limitDate;
                    hasLimit = true;
                }
                begin = this.calculateIndex(StringUtil.removeBackquote(formatter.get().format(endTime)));
                if (begin == null || end == null) {
                    return re;
                }
                if (end >= begin) {
                    int len = end - begin + 1;
                    if (hasLimit) {
                        re = new Integer[len + 1];
                        re[0] = 0;
                        for (int i = 0; i < len; i++) {
                            re[i + 1] = begin + i;
                        }
                    } else {
                        re = new Integer[len];
                        for (int i = 0; i < len; i++) {
                            re[i] = begin + i;
                        }
                    }
                    return re;
                } else {
                    return re;
                }
            }
        } catch (ParseException e) {
            throw new IllegalArgumentException(new StringBuilder().append("endValue:").append(endValue).append(" Please check if the format satisfied.").toString(), e);
        }
        return targetPartition;
    }

    @Override
    public List<Partition> calculateRange(String beginValue, String endValue) {
        Integer[] integers = calculateIndexRange0(beginValue, endValue);
        List<Partition> partitions = tableHandler.dataNodes();
        if (integers == null) {
            return new ArrayList<>(partitions);
        }
        ArrayList<Partition> objects = new ArrayList<>();
        for (Integer integer : integers) {
            objects.add(partitions.get(integer));
        }
        return objects;
    }

    public void setsPartionDay(String sPartionDay) {
        this.sPartionDay = sPartionDay;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getsLastDay() {
        return sLastDay;
    }

    public void setsLastDay(String sLastDay) {
        this.sLastDay = sLastDay;
    }

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {

        this.tableHandler = tableHandler;
        this.properties = properties;
        this.ranges = ranges;

        this.dateFormat = Objects.toString(properties.get("dateFormat"));
        this.sLastDay = (Objects.toString(properties.get("lastTime")));
        this.sPartionDay = Objects.toString(properties.get("partionDay"));


        try {
            formatter = new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat(dateFormat);
                }
            };
            sLastTime = Integer.valueOf(sLastDay);
            partionTime = Integer.parseInt(sPartionDay) * oneDay;
        } catch (Exception e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }

    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+ dateFormat + sLastDay + sPartionDay;
    }
}
