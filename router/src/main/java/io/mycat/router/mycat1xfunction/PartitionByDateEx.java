package io.mycat.router.mycat1xfunction;

import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class PartitionByDateEx extends Mycat1xSingleValueRuleFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByDate.class);

    private String sBeginDate;
    private String sEndDate;
    private String sPartionDay;
    private String dateFormat;

    private long beginDate;
    private long partionTime;
    private long endDate;
    private int nCount;

    private ThreadLocal<SimpleDateFormat> formatter;

    private static final long oneDay = 86400000;
    //支持自然日分区属性
    private String sNaturalDay;
    //是否自然日分区
    private boolean bNaturalDay;
    //自然日差额最少是28天
    private static final int naturalLimitDay = 28;
    //开启自然日模式
    private static final String naturalDayOpen = "1";

    private String oldsPartionDay;


    public void init() {
        try {
            //Support  Natural Day
            if (naturalDayOpen.equals(sNaturalDay)) {
                bNaturalDay = true;
                oldsPartionDay = sPartionDay;
                sPartionDay = "1";
            }
            if (sBeginDate != null && !sBeginDate.equals("")) {
                partionTime = Integer.parseInt(sPartionDay) * oneDay;
                beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();
            }
            if (sEndDate != null && !sEndDate.equals("") && beginDate > 0) {
                endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                nCount = (int) ((endDate - beginDate) / partionTime) + 1;
                if (bNaturalDay && nCount < naturalLimitDay) {
                    bNaturalDay = false;
                    partionTime = Integer.parseInt(oldsPartionDay) * oneDay;
                    nCount = (int) ((endDate - beginDate) / partionTime) + 1;
                }
            }
            formatter = new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat(dateFormat);
                }
            };
        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }

    @Override
    public String name() {
        return "PartitionByDateEx";
    }

    @Override
    public int calculateIndex(String columnValue) {
        int index;
        try {
            int targetPartition;
            if (bNaturalDay) {
                Calendar curTime = Calendar.getInstance();
                curTime.setTime(formatter.get().parse(columnValue));
                targetPartition = curTime.get(Calendar.DAY_OF_MONTH);
                return targetPartition - 1;
            }
            long targetTime = formatter.get().parse(columnValue).getTime();
            targetPartition = (int) ((targetTime - beginDate) / partionTime);
            if (targetTime > endDate && nCount != 0) {
                targetPartition = targetPartition % nCount;
            }
            return targetPartition;
        } catch (ParseException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please check if the format satisfied.", e);
        }
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        SimpleDateFormat format = new SimpleDateFormat(this.dateFormat);
        try {
            Date beginDate = format.parse(beginValue);
            Date endDate = format.parse(endValue);
            Calendar cal = Calendar.getInstance();
            List<Integer> list = new ArrayList<Integer>();
            while (beginDate.getTime() <= endDate.getTime()) {
                int nodeValue = this.calculateIndex(format.format(beginDate));
                if (Collections.frequency(list, nodeValue) < 1) list.add(nodeValue);
                cal.setTime(beginDate);
                cal.add(Calendar.DATE, 1);
                beginDate = cal.getTime();
            }

            int[] nodeArray = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                nodeArray[i] = list.get(i);
            }

            return nodeArray;
        } catch (ParseException e) {
            LOGGER.error("error", e);
            return null;
        }
    }


    public void setsBeginDate(String sBeginDate) {
        this.sBeginDate = sBeginDate;
    }

    public void setsPartionDay(String sPartionDay) {
        this.sPartionDay = sPartionDay;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getsEndDate() {
        return this.sEndDate;
    }

    public void setsEndDate(String sEndDate) {
        this.sEndDate = sEndDate;
    }

    public String getsNaturalDay() {
        return sNaturalDay;
    }

    public void setsNaturalDay(String sNaturalDay) {
        this.sNaturalDay = sNaturalDay;
    }

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {
        setDateFormat((String) properties.get("dateFormat"));
        setsBeginDate((String) properties.get("beginDate"));
        setsEndDate((String) properties.get("endDate"));
        setsPartionDay(Objects.toString(properties.get("partionDay")));
        setsNaturalDay(Objects.toString(properties.get("naturalDay")));
        init();
    }

    @Override
    public String getErUniqueID() {
        return Objects.hash(sBeginDate, sEndDate, sPartionDay, dateFormat, beginDate, partionTime, endDate, nCount, formatter, sNaturalDay, bNaturalDay, oldsPartionDay) + "";
    }
}
