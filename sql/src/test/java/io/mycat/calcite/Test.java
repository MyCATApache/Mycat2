package io.mycat.calcite;

import io.mycat.calcite.table.JdbcTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    public static void main(String args[]) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection);


        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
        final JdbcTable table = new JdbcTable("s", "test");
        final JdbcTable table2 = new JdbcTable("s", "test1");

        schema.add("test", table);
        schema.add("test1", table);

        // final String sql = "select * from \"s\".\"test\" where \"pk\" > 2 ";
        final String sql = "select * from \"s\".\"test\"  as t1 full join \"s\".\"test1\" as t2 on t1.\"pk\" > t2.\"pk\"";


        ResultSet rs = connection.createStatement().executeQuery(sql);
        int count = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <=count ; i ++ )
            {
                System.out.print(rs.getString(i) + "    ");
            }
            System.out.println(" ");
        }

    }
}
