package io.mycat.router.custom;

import io.mycat.Partition;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultCustomRuleFunctionTest {

    @Test
    public void test(){
        MergeSubTablesFunction mergeSubTablesFunction = new MergeSubTablesFunction();
        HashMap<String, Object> map = new HashMap<>();
        map.put("tablePrefix","prefix_");
        map.put("beginIndex","1");
        map.put("endIndex","2");
        map.put("targetName","defaultDs");
        map.put("schemaName","db1");
        map.put("tableName","travelrecord");
        map.put("segmentQuery",Boolean.TRUE.toString());

        mergeSubTablesFunction.init(null,map, new HashMap<>());

        Partition partition = mergeSubTablesFunction.calculate("11");
        Assert.assertEquals(partition.getTargetName(),"defaultDs");
        Assert.assertEquals(partition.getSchema(),"db1");
        Assert.assertEquals(partition.getTable(),"prefix_1");

        List<Partition> partitions = mergeSubTablesFunction.calculateRange("11", "13");
        List<String> targets = partitions.stream().map(i -> i.getTargetName()).collect(Collectors.toList());
        Assert.assertEquals("[defaultDs, defaultDs, defaultDs]",targets.toString());
        List<String> tables = partitions.stream().map(i -> i.getTargetSchemaTable()).collect(Collectors.toList());
        Assert.assertEquals("[db1.prefix_1, db1.prefix_2, db1.prefix_3]",tables.toString());
    }

    @Test
    public void test2(){
        MergeSubTablesFunction mergeSubTablesFunction = new MergeSubTablesFunction();
        HashMap<String,Object> map = new HashMap<>();
        map.put("tablePrefix","prefix_");
        map.put("beginIndex","1");
        map.put("endIndex","2");
        map.put("targetName","defaultDs");
        map.put("schemaName","db1");
        map.put("tableName","travelrecord");
        map.put("segmentQuery",Boolean.FALSE.toString());

        mergeSubTablesFunction.init(null,map, new HashMap<>());

        Partition partition = mergeSubTablesFunction.calculate("11");
        Assert.assertEquals(partition.getTargetName(),"defaultDs");
        Assert.assertEquals(partition.getSchema(),"db1");
        Assert.assertEquals(partition.getTable(),"prefix_1");

        List<Partition> partitions = mergeSubTablesFunction.calculateRange("11", "13");
        List<String> targets = partitions.stream().map(i -> i.getTargetName()).collect(Collectors.toList());
        Assert.assertEquals("[defaultDs]",targets.toString());
        List<String> tables = partitions.stream().map(i -> i.getTargetSchemaTable()).collect(Collectors.toList());
        Assert.assertEquals("[db1.travelrecord]",tables.toString());
    }
}