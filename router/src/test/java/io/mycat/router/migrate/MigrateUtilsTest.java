package io.mycat.router.migrate;

import com.google.common.collect.Lists;
import io.mycat.router.function.NodeIndexRange;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 迁移任务
 * Created by magicdoom on 2016/9/16.
 * @author chenjunwen refactor
 */
public class MigrateUtilsTest {
    @Test
    public void balanceExpand() {
        Map<Integer, List<NodeIndexRange>> integerListMap = new TreeMap<>();
        integerListMap.put(0,Lists.newArrayList(new NodeIndexRange(0,0,32))) ;
        integerListMap.put(1,Lists.newArrayList(new NodeIndexRange(1,33,65))) ;
        integerListMap.put(2,Lists.newArrayList(new NodeIndexRange(2,66,99))) ;
        int totalSlots=100;
        List<String> oldDataNodes = Lists.newArrayList("dn1","dn2","dn3");
        List<String> newDataNodes =  Lists.newArrayList("dn4","dn5");

        Map<String, List<MigrateTask>> tasks= MigrateUtils
                .balanceExpand( integerListMap, oldDataNodes, newDataNodes,totalSlots);
        System.out.println(tasks);
    }
}