package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class HBTChecker  extends BaseChecker{
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
        check("select * from db1.travelrecord", "");

        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" +max+ ",999)");
        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" +min+ ",999)");

        //fromTable
        checkHbt("fromTable(db1,travelrecord)","(1,999,null,null,null,null)(999999999,999,null,null,null,null)");

        //table
        checkHbt("table(fields(fieldType(id,integer)),values(1,2,3))","(1)(2)(3)");

        //fromSql
        checkHbt("fromSql('defaultDs','select 1')","(1)");

        //map
        checkHbt("table(fields(fieldType(id,integer),fieldType(id2,integer)),values(1,2)).map(id + id2,id)","(3,1)");

        //rename
        checkHbt("table(fields(fieldType(`1`,`integer`,true),fieldType(`2`,`integer`,true)),values(1,2)).rename(`2`,`1`)\n","(1,2)");

        //filter
        checkHbt("fromTable(db1,travelrecord).filter(`id` = 1)","(1,999,null,null,null,null)");

        //集合操作测试
        checkHbt("unionAll(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 2'))","(1)(2)");
        checkHbt("unionDistinct(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 1'))","(1)");
        checkHbt("exceptDistinct(fromSql('defaultDs','select 1'),fromSql('defaultDs2','select 1'))","");
        checkHbt("exceptAll(fromSql('defaultDs','select 2'),fromSql('defaultDs','select 2'))","()");
        checkHbt("intersectAll(fromSql('defaultDs','select 1'),fromSql('defaultDs','select 2'))","()");
        checkHbt("intersectDistinct(fromSql('defaultDs','select 2'),fromSql('defaultDs','select 2'))","(2)");
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