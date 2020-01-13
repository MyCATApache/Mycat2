package io.mycat.router.function;

import io.mycat.router.RuleFunction;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * 自动迁移御用分片算法，预分slot 102400个，映射到dn上，再conf下会保存映射文件，请不要修改
 *
 * @author nange magicdoom@gmail.com
 * @author chenjunwen
 */
public class PartitionByCRC32PreSlot extends RuleFunction {

    private static final int DEFAULT_SLOTS_NUM = 102400;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private final int[] rangeMap2 = new int[DEFAULT_SLOTS_NUM];

    @Override
    public String name() {
        return "PartitionByCRC32PreSlot";
    }

    @Override
    public int calculate(String columnValue) {
        if (columnValue == null) {
            throw new IllegalArgumentException();
        }
        PureJavaCrc32 crc32 = new PureJavaCrc32();
        byte[] bytes = columnValue.getBytes(DEFAULT_CHARSET);
        crc32.update(bytes, 0, bytes.length);
        long x = crc32.getValue();
        int slot = (int) (x % DEFAULT_SLOTS_NUM);
        return rangeMap2[slot];
    }

    @Override
    public int[] calculateRange(String beginValue, String endValue) {
        return calculateRange(beginValue, endValue);
    }

    @Override
    public int getPartitionNum() {
        return rangeMap2.length;
    }

    @Override
    protected void init(Map<String, String> prot, Map<String, String> ranges) {
        String countText = prot.get("count");
        NodeIndexRange[] longRanges;
        if (countText != null) {
            int count = Integer.parseInt(countText);
            int slotSize = DEFAULT_SLOTS_NUM / count;
            longRanges = new NodeIndexRange[count];
            for (int i = 0; i < count; i++) {
                if (i == count - 1) {
                    longRanges[i]= new NodeIndexRange(i,i * slotSize,(DEFAULT_SLOTS_NUM - 1));
                } else {
                    longRanges[i]= new NodeIndexRange(i,i * slotSize,((i + 1) * slotSize - 1));
                }
            }
        } else {
            longRanges = NodeIndexRange.getLongRanges(ranges);
        }
        for (NodeIndexRange longRange : longRanges) {
            int valueStart = (int) longRange.valueStart;
            int valueEnd = (int) longRange.valueEnd;
            int nodeIndex = longRange.nodeIndex;
            for (int i = valueStart; i <= valueEnd; i++) {
                rangeMap2[i] = nodeIndex;
            }
        }
    }
}