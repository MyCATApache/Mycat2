package io.mycat.calcite;

import io.mycat.calcite.table.JdbcTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.sql.*;

public class Test {
    public static void main(String args[]) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection);


        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        //rootSchema.add("TEST", MetaData.getMetaData().schema.schema);
        MetaData.getMetaData(calciteConnection);
        /*
        SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
        final JdbcTable table = new JdbcTable("s", "test");
        final JdbcTable table2 = new JdbcTable("s", "test1");

        schema.add("test", table);
        schema.add("test1", table);

         */
        // final String sql = "select * from test.TRAVELRECORD ";
        //final String sql = "select * from \"s\".\"test\"  as t1 full join \"s\".\"test1\" as t2 on t1.\"pk\" > t2.\"pk\"";

     //   final String sql = "select * from test.TRAVELRECORD where test.TRAVELRECORD.ID = 1";
     //   final String sql = "select * from test.TRAVELRECORD as tr join test.t as t on tr.ID= t.ID where tr.ID = 1";
        final String sql = "select * from test.TRAVELRECORD ";
        //final String sql = "select * from test.score";
        Statement statement = connection.createStatement();
        System.out.println(sql);
        ResultSet resultSet =statement.executeQuery("explain plan FOR " + sql);
        while (resultSet.next()){
            String string = resultSet.getString(1);
            System.out.println(string);
        }
        ResultSet rs = connection.createStatement().executeQuery(sql);
        System.out.println(sql);
        int count = rs.getMetaData().getColumnCount();
        for (int i = 1; i <=count; i++) {
            System.out.print(rs.getMetaData().getColumnName(i) + "    ");
        }

        System.out.println();
        for (int i = 1; i <=count; i++) {
            System.out.print(rs.getMetaData().getCatalogName(i) + "    ");
        }
        System.out.println();
        for (int i = 1; i <=count; i++) {
            System.out.print(rs.getMetaData().getSchemaName(i) + "    ");
        }
        System.out.println();
        for (int i = 1; i <=count; i++) {
            System.out.print(rs.getMetaData().getTableName(i) + "    ");
        }
        System.out.println();
        for (int i = 1; i <=count; i++) {
            System.out.print(rs.getMetaData().getColumnTypeName(i) + "    ");
        }

        for (int i = 1; i <=count; i++) {
            System.out.print(rs.getMetaData().getColumnType(i) + "    ");
        }
        System.out.println();

        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <=count ; i ++ )
            {
                System.out.print(rs.getString(i) + "    ");
            }
            System.out.println(" ");
        }

    }
}
