package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class SQLExprChecker extends BaseChecker {
    public SQLExprChecker(Statement statement) {
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
        check("delete from db1.company");
        check("select id from db1.company","");

        //测试插入
        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES (999)");
        Assert.assertEquals(ok.getUpdateCount(), 1);
        checkContains("select * from db1.travelrecord where user_id = 999", ",999,null,null,null,null");//出现自增序列
        checkContains("select * from db1.travelrecord where id = " + ok.getLastId(), ",999,null,null,null,null");
        check("delete from db1.travelrecord where id = " + ok.getLastId());
        check("select * from db1.travelrecord", "");

        //自定义给与的自增序列
        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" +max+ ",999)");
        Assert.assertEquals(ok.getUpdateCount(), 1);
        Assert.assertEquals(ok.getLastId(), max);
        checkContains("select * from db1.travelrecord where id = " + ok.getLastId(), ",999,null,null,null,null");
        check("delete from db1.travelrecord where id = " + ok.getLastId());
        check("select * from db1.travelrecord", "");

        //进阶测试(查询)
        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" +max+ ",999)");
        ok = executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" +min+ ",999)");

        /*
        SELECT [ALL | DISTINCT]
         */
        check("select distinct(user_id) from db1.travelrecord", "999");
        check("select all(user_id) from db1.travelrecord", "(999)(999)");

        /*
        SELECT [ALL | DISTINCT] select_expr [, select_expr ...]  FROM table_references
         */
        //     算术表达式测试
        check("select user_id+id from db1.travelrecord",999+max+")("+(999+min));
        check("select user_id-id from db1.travelrecord",(999-max)+")("+(999-min));
        check("select user_id*id from db1.travelrecord",(999*max)+")("+(999*min));
        check("select (user_id*1.0)/(id*1.0) from db1.travelrecord");//结果不确定
//        check("select user_id DIV id from db1.travelrecord",(999/max)+")("+(999/min));不支持
        check("select user_id % id from db1.travelrecord",(999%max)+")("+(999%min));
        check("select user_id MOD id from db1.travelrecord",(999%max)+")("+(999%min));

        //

          /*
        SELECT [ALL | DISTINCT] select_expr [, select_expr ...]  FROM table_references
         */
        check("delete from db1.company");
        check("select id from db1.company","");
        check("select user_id from db1.travelrecord","(999)(999)");
        check("select user_id from db1.travelrecord,db1.company","");

        /*
        SELECT [ALL | DISTINCT] select_expr [, select_expr ...] [FROM table_references  [WHERE where_condition]
        逻辑运算符 测试
         */
        check("select id,user_id from db1.travelrecord where id = "+min,"(" +min+ ",999)");
        check("select id,user_id from db1.travelrecord where id = "+max,"(" +max+ ",999)");

        //or表达式
        check("select id,user_id from db1.travelrecord where id = "+min+" or "+" id = "+max,"(" +min+ ",999)"+"(" +max+ ",999)");

        //and表达式
        check("select id,user_id from db1.travelrecord where id = "+min+" and "+" user_id = 999","(" +min+ ",999)");

        //not表达式
        check("select id,user_id from db1.travelrecord where !(id = "+min+" and "+" user_id = 999"+")","(" +max+ ",999)");

        //between
        check("select id,user_id from db1.travelrecord where id between 1 and 999","(" +min+ ",999)");

        //like
        check("select id,user_id from db1.travelrecord where user_id LIKE '99%'");

        //xor表达式 不支持
//        check("select id,user_id from db1.travelrecord where (id = "+min+" xor "+" user_id = 999"+")","(" +max+ ",999)");
    }

    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            for (String u : initList) {
                statement.execute(u);
            }
            BaseChecker checker = new SQLExprChecker(statement);
            checker.run();
        }
    }
}