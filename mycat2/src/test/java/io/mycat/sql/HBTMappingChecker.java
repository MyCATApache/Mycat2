package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;


public class HBTMappingChecker  extends BaseChecker {

    public HBTMappingChecker(Statement statement) {
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
            BaseChecker checker = new HBTMappingChecker(statement);
            checker.run();
        }
    }

    @Override
    public void run() {
        //explainHbt("leftJoin(`$0` eq `$$0`,leftJoin(`$0` eq `$$0`,fromTable(db1,travelrecord), fromTable(db1,travelrecord)),fromTable(db1,company))", "");
    }
}