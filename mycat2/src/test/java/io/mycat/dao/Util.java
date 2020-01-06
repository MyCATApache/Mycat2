package io.mycat.dao;

import io.mycat.DesRelNodeHandler;
import io.mycat.util.DumpUtil;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;

public class Util {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        String username = "root";
        String password = "123456";

        Properties properties = new Properties();
        properties.put("user",username);
        properties.put("password", password);
        properties.put("useBatchMultiSend", "false");
        properties.put("usePipelineAuth", "false");

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://0.0.0.0:8066/scott?useServerPrepStmts=false&useCursorFetch=true&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8", properties);
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        try(Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select 1");

            String s = getString(resultSet);
            System.out.println(s);
        }
        connection.setAutoCommit(false);
        Statement statement1 = connection.createStatement();
        boolean execute = statement1.execute("INSERT INTO `db1`.`travelrecord` (`id`, `d`) VALUES (1, X'89'); ", Statement.RETURN_GENERATED_KEYS);
        connection.rollback();

    }

    private static String getString(ResultSet resultSet) throws SQLException {
        CharArrayWriter writer = new CharArrayWriter(8192);
        DesRelNodeHandler.            dump(resultSet, true, new PrintWriter(writer));
        return writer.toString();
    }

}