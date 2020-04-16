package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class DateChecker extends BaseChecker{

    public DateChecker(Statement statement) {
        super(statement);
    }

    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            for (String u : initList) {
                statement.execute(u);
            }
            BaseChecker checker = new DateChecker(statement);
            checker.run();
        }
    }

    @Override
    public void run() {
        check("delete from db1.travelrecord");
        executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (1,999)");

        simplyCheck("CURDATE()", LocalDate.now().toString());//
        simplyCheck("curdate()", LocalDate.now().toString());//
        simplyCheck("now()");//

    }
}