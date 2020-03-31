package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class HBTChecker extends BaseChecker {
    public HBTChecker(Statement statement) {
        super(statement);
    }

    @Override
    public void run() {
        long max = 999999999;
        long min = 1;
        Ok ok;
        //分布式引擎基础测试
        //清除所有数据


        check("delete from db1.travelrecord");
        check("delete from db1.company");
        executeUpdate("INSERT INTO `db1`.`company` (`id`, `companyname`, `addressid`) VALUES ('1','Intel','1'),('2','IBM','2'),('3','Dell','3')");
        check("select * from db1.travelrecord", "");

        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" + max + ",999)");
        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" + min + ",999)");

        //join
        checkHbt("innerJoin(`id0` = `id`,fromTable(db1,travelrecord)\n" +
                "          .map(`id` as `id0`),fromTable(db1,company))", "(1,1,Intel,1)");

        //fromTable
        checkHbt("fromTable(db1,travelrecord)", "(1,999,null,null,null,null)(999999999,999,null,null,null,null)");

        //table
        checkHbt("table(fields(fieldType(id,integer)),values(1,2,3))", "(1)(2)(3)");

        //fromSql
        checkHbt("fromSql('defaultDs','select 1')", "(1)");

        //map
        checkHbt("table(fields(fieldType(id,integer),fieldType(id2,integer)),values(1,2)).map(id + id2,id)", "(3,1)");

        //rename
        checkHbt("table(fields(fieldType(`1`,`integer`,true),fieldType(`2`,`integer`,true)),values(1,2)).rename(`2`,`1`)\n", "(1,2)");

        //filter
        checkHbt("fromTable(db1,travelrecord).filter(`id` = 1)", "(1,999,null,null,null,null)");


        //集合操作测试
        checkHbt("unionAll(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 2'))", "(1)(2)");

        //distinct
        checkHbt("unionAll(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 1')).distinct()", "(1)");

        //groupBy
        checkHbt("fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`)),aggregating(avg(`id`))))", "(1,1.0)(999999999,9.99999999E8)");
        checkHbt("fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`,`user_id`)),aggregating(avg(`id`))))", "(999999999,999,9.99999999E8)(1,999,1.0)");


        checkHbt("fromTable(db1,travelrecord).groupBy(keys(groupKey()),aggregating(sum(`id`)))", "(1000000000)");

        checkHbt("unionDistinct(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 1'))", "(1)");
        checkHbt("exceptDistinct(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 1'))", "");
        checkHbt("exceptAll(fromSql('defaultDs','select 2'),fromSql('defaultDs','select 2'))", "()");
        checkHbt("intersectAll(fromSql('defaultDs','select 1'),fromSql('defaultDs','select 2'))", "()");
        checkHbt("intersectDistinct(fromSql('defaultDs','select 2'),fromSql('defaultDs','select 2'))", "(2)");

        //EXPLAIN SELECT MAX(id) FROM db1.travelrecord;
        checkHbt("fromTable(db1,travelrecord).groupBy(keys(groupKey()),aggregating(max(`id`)))", "(999999999)");

        //EXPLAIN SELECT COUNT(*) FROM db1.travelrecord;
        checkHbt("fromTable(db1,travelrecord).groupBy(keys(groupKey()),aggregating(count()))", "(2)");

        //EXPLAIN SELECT COUNT(1) FROM db1.travelrecord;
        checkHbt("unionAll( fromSql(defaultDs2,'SELECT COUNT(*)  FROM `db2`.`travelrecord`'), fromSql(defaultDs,'SELECT COUNT(*)  FROM (SELECT NULL  FROM `db1`.`travelrecord`  UNION ALL  SELECT NULL  FROM `db1`.`travelrecord2`  UNION ALL  SELECT NULL  FROM `db1`.`travelrecord3`) AS `t2`')).groupBy(keys(groupKey()),aggregating(count()))",
                "(2)"
        );


        //order by
        checkHbt("fromTable(db1,travelrecord).orderBy(order(id,ASC))", "(1,999,null,null,null,null)(999999999,999,null,null,null,null)");
        checkHbt("fromTable(db1,travelrecord).orderBy(order(id,DESC))", "(999999999,999,null,null,null,null)(1,999,null,null,null,null)");

        //limit
        checkHbt("fromTable(db1,travelrecord).limit(0,1)", "(1,999,null,null,null,null)");
        checkHbt("fromTable(db1,travelrecord).limit(1,1)", "(999999999,999,null,null,null,null)");


        checkHbt("leftJoin(`id0` = `id`,fromTable(db1,travelrecord)\n" +
                "          .map(`id` as `id0`),fromTable(db1,company))", "(1,1,Intel,1)(999999999,null,null,null)");
        checkHbt("rightJoin(`id0` = `id`,fromTable(db1,travelrecord)\n" +
                "          .map(`id` as `id0`),fromTable(db1,company))", "(1,1,Intel,1)(null,2,IBM,2)(null,3,Dell,3)");
        checkHbt("semiJoin(`id0` = `id`,fromTable(db1,travelrecord)\n" +
                "          .map(`id` as `id0`),fromTable(db1,company))", "(1)");
        checkHbt("antiJoin(`id0` = `id`,fromTable(db1,travelrecord)\n" +
                "          .map(`id` as `id0`),fromTable(db1,company))", "(999999999)");
        checkHbt("antiJoin(`id0` = `id`,fromTable(db1,travelrecord)\n" +
                "          .map(`id` as `id0`),fromTable(db1,company))", "(999999999)");

        //三表
        checkHbt("leftJoin(`$0` eq `$$3`," +
                        "leftJoin(`$0` eq `$$3`," +
                        "unionAll( fromSql(defaultDs2,'SELECT *  FROM `db2`.`travelrecord`'), " +
                        "fromSql(defaultDs,'SELECT *  FROM `db1`.`travelrecord`  UNION ALL  SELECT *  FROM `db1`.`travelrecord2`  UNION ALL  SELECT *  FROM `db1`.`travelrecord3`'))," +
                        "fromSql(defaultDs2,'SELECT `id`, `companyname`, `addressid`, CAST(`id` AS SIGNED) AS `id0`  FROM `db2`.`company`'))" +
                        ".map(`$0` as `id`,`$1` as `user_id`,`$2` as `traveldate`,`$3` as `fee`,`$4` as `days`,`$5` as `blob`,`$6` as `id0`,`$7` as `companyname`,`$8` as `addressid`)" +
                        ",fromSql(defaultDs2,'SELECT `id`, `companyname`, `addressid`, CAST(`id` AS SIGNED) AS `id0`  FROM `db2`.`company`'))" +
                        ".map(`$0` as `id`,`$1` as `user_id`,`$2` as `traveldate`,`$3` as `fee`,`$4` as `days`,`$5` as `blob`,`$6` as `id0`,`$7` as `companyname`,`$8` as `addressid`,`$9` as `id1`,`$10` as `companyname0`,`$11` as `addressid0`)",
                "(999999999,999,null,null,null,null,null,null,null,null,null,null)(1,999,null,null,null,null,1,Intel,1,1,Intel,1)");

        checkHbt("leftJoin(`$0` eq `$$0`," +
                        "leftJoin(`$0` eq `$$0`,fromTable(db1,travelrecord), fromTable(db1,travelrecord))" +
                        ",fromTable(db1,company))",
                "(1,999,null,null,null,null,1,999,null,null,null,null,1,Intel,1)(999999999,999,null,null,null,null,999999999,999,null,null,null,null,null,null,null)");
        checkHbt("filterFromTable(`id` = 1,db1,travelrecord)", "(1,999,null,null,null,null)");
        checkHbt("fromRelToSql(defaultDs,fromTable('db1','travelrecord').filter(`id` = 1).map(`id`))", "(1)");
        checkHbt("modifyFromSql(defaultDs,'delete from db1.travelrecord3')", "(0,0)");
    }

    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            for (String u : initList) {
                statement.execute(u);
            }
            BaseChecker checker = new HBTChecker(statement);
            checker.run();
        }
    }
}