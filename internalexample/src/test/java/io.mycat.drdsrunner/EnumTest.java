package io.mycat.drdsrunner;

import io.mycat.MetadataManager;
import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.RangeVariableType;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.ShardingFunction;
import io.mycat.config.ShardingTableConfig;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.function.AutoFunction;
import io.mycat.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.util.*;
import java.util.function.ToIntFunction;

public class EnumTest extends AutoFunctionFactoryTest {
    @Test
    public void testHashMM() {
        ShardingTableConfig mainSharding = new ShardingTableConfig();
        mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by mod_hash(id) tbpartition by MM(traveldate) tbpartitions 2 dbpartitions 4;");
        mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                "\t\t\t\t\t\"tableNum\":\"4\",\n" +
                "\t\t\t\t\t\"tableMethod\":\"MM(traveldate)\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();

        LocalDate start = LocalDate.of(2021, 11, 8);

        List<Partition> calculate = shardingFuntion.calculate(Collections.singletonMap("traveldate",
                (new RangeVariable("traveldate", RangeVariableType.EQUAL, start))));
        Assert.assertEquals(2, calculate.size());
        for (Partition partition : calculate) {
            Assert.assertEquals(start.getMonthValue() % 4, (int) partition.getTableIndex());
        }

        {
            LocalDate end = LocalDate.of(2021, 11, 9);

            List<Partition> calculate2 = shardingFuntion.calculate(Collections.singletonMap("traveldate",
                    (new RangeVariable("traveldate", RangeVariableType.RANGE, start, end))));

            Assert.assertEquals(calculate, calculate2);

        }

        {
            LocalDate end = LocalDate.of(2021, 12, 9);

            List<Partition> calculate2 = shardingFuntion.calculate(Collections.singletonMap("traveldate",
                    (new RangeVariable("traveldate", RangeVariableType.RANGE, start, end))));


            HashSet<Integer> set = new HashSet<>();
            for (int i = 11; i <=12 ; i++) {
                set.add(i%4);
            }
           Assert.assertEquals(true, calculate2.stream().allMatch(i->set.contains(i.getTableIndex())));
            System.out.println();
        }

        {
            LocalDate end = LocalDate.of(2022, 1, 9);

            List<Partition> calculate2 = shardingFuntion.calculate(Collections.singletonMap("traveldate",
                    (new RangeVariable("traveldate", RangeVariableType.RANGE, start, end))));


            HashSet<Integer> set = new HashSet<>();
            for (Integer integer : Arrays.asList(11, 12, 1)) {
                set.add(integer%4);
            }

            Assert.assertEquals(true, calculate2.stream().allMatch(i->set.contains(i.getTableIndex())));
            System.out.println();
        }


    }

    @Test
    public void testYYYYWEEK() {
        ShardingTableConfig mainSharding = new ShardingTableConfig();
        mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by YYYYWEEK(traveldate) tbpartition by YYYYWEEK(traveldate) tbpartitions 2 dbpartitions 4;");
        mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"dbNum\":\"8\",\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                "\t\t\t\t\t\"tableNum\":\"14\",\n" +
                "\t\t\t\t\t\"tableMethod\":\"YYYYWEEK(traveldate)\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t\t\"dbMethod\":\"YYYYWEEK(traveldate)\"\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();

//        LocalDate start = LocalDate.of(2021, 11, 8);
//
//        List<Partition> calculate = shardingFuntion.calculate(Collections.singletonMap("traveldate",
//                (new RangeVariable("traveldate", RangeVariableType.EQUAL, start))));
//        Assert.assertEquals(1, calculate.size());
//        Assert.assertEquals( 7, (int) calculate.get(0).getTableIndex());
//        Assert.assertEquals( 3, (int) calculate.get(0).getDbIndex());
//        {
//            LocalDate end = LocalDate.of(2021, 11, 9);
//
//            List<Partition> calculate2 = shardingFuntion.calculate(Collections.singletonMap("traveldate",
//                    (new RangeVariable("traveldate", RangeVariableType.RANGE, start, end))));
//
//            Assert.assertEquals(calculate, calculate2);
//
//        }
//
//        AutoFunction autoFunction = (AutoFunction) shardingFuntion;
//
//        ArrayList<Object> res = new ArrayList<>();
//        Optional<Set<LocalDate>> integers = autoFunction.enumMonthValue(12, new ToIntFunction<Object>() {
//            @Override
//            public int applyAsInt(Object value) {
//                res.add(value);
//                return ((LocalDate)value).getMonthValue();
//            }
//        }, LocalDate.of(2021, 11, 1), LocalDate.of(2022, 3, 1));
//
//        Assert.assertEquals("[2021-11-01, 2021-12-01, 2022-01-01, 2022-02-01]",res.toString());
//        Assert.assertEquals("[1, 2, 11, 12]",integers.get().toString());

        {
            List<Partition> calculate2 = shardingFuntion.calculate(Collections.singletonMap("traveldate",
                    (new RangeVariable("traveldate", RangeVariableType.RANGE,
                            LocalDate.of(2021, 11, 1),
                            LocalDate.of(2022, 3, 1)))));
            Assert.assertEquals(18,calculate2.size());
            System.out.println("");
        }
        System.out.println();
    }
}
