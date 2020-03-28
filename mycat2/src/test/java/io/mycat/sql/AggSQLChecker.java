package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class AggSQLChecker extends BaseChecker  {
    public AggSQLChecker(Statement statement) {
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

        /*
        SELECT
    [ALL | DISTINCT]
    select_expr [, select_expr ...]
    [FROM table_references
    [WHERE where_condition]
    [GROUP BY {col_name | expr | position}
    [HAVING where_condition]
    [ORDER BY {col_name | expr | position}
      [ASC | DESC], ...]
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]
         */
        check("select id from db1.travelrecord GROUP BY id", "(1)(999999999)");
        check("select id,COUNT(user_id) from db1.travelrecord GROUP BY id", "(1,1)(999999999,1)");
        check("select id,COUNT(DISTINCT user_id) from db1.travelrecord GROUP BY id", "(1,1)(999999999,1)");
        check("select MAX(id) from db1.travelrecord", "(999999999)");
        check("select MIN(id) from db1.travelrecord", "(1)");
        check("select id,sum(id) from db1.travelrecord GROUP BY id", "(1,1)(999999999,999999999)");
        check("select id,avg(id) from db1.travelrecord GROUP BY id", "(1,1)(999999999,999999999)");
    }

    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            for (String u : initList) {
                statement.execute(u);
            }
            BaseChecker checker = new AggSQLChecker(statement);
            checker.run();
        }
    }
}