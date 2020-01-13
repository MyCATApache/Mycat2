package io.mycat.router.migrate;

import com.google.common.collect.Lists;
import io.mycat.router.NodeIndexRange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * 迁移任务
 * Created by magicdoom on 2016/9/16.
 * @author chenjunwen refactor
 */
public class MigrateUtilsTest {
    @Test
    public void balanceExpand() {
     List<List<NodeIndexRange>> integerListMap = new ArrayList<>();
        integerListMap.add(Lists.newArrayList(new NodeIndexRange(0,0,32))) ;
        integerListMap.add(Lists.newArrayList(new NodeIndexRange(1,33,65))) ;
        integerListMap.add(Lists.newArrayList(new NodeIndexRange(2,66,99))) ;
        int totalSlots=100;
        List<String> oldDataNodes = Lists.newArrayList("dn1","dn2","dn3");
        List<String> newDataNodes =  Lists.newArrayList("dn4","dn5");

        SortedMap<String, List<MigrateTask>> tasks= MigrateUtils.balanceExpand( integerListMap, oldDataNodes, newDataNodes,totalSlots);
        System.out.println(tasks);
    }
}