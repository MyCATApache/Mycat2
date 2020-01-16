package io.mycat.calcite;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyCatResultSetEnumerable<T> extends AbstractEnumerable<T> {
    private ResultSet rs;
    List<ResultSet> rss = new ArrayList<>();
    private Connection connection;
    private Function1<ResultSet, Function0<T>> rowBuilderFactory;
    private  Function0<T> rowBuilder;
    private String filterSql;

    public MyCatResultSetEnumerable(BackEndTableInfo[] info, Function1<ResultSet, Function0<T>> rowBuilderFactory, String filterSql) {
        this.connection = connection;
        this.rowBuilderFactory = rowBuilderFactory;
        this.filterSql = filterSql;

        try {
            System.out.println("run query");
            rss = new ArrayList<>();
            for (int i = 0; i <info.length; i++) {
                connection = DriverManager
                        .getConnection("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=UTC",
                                "root","123456");
                String sql;
                if (filterSql != null) {
                    sql = "select * from " + info[i].schemaName + "." + info[i].tableName + " where " + filterSql;
                }
                else {
                    sql = "select * from " +info[i].schemaName + "." + info[i].tableName;
                }
                System.out.println("get data using : " + sql);
                rs = connection.createStatement().executeQuery(sql);
                rss.add(rs);
            }

            //rs = connection.createStatement().executeQuery("select * from test.test");
            //rss.add(rs);
            //rss.add(connection.createStatement().executeQuery("select * from test.test1"));

            //rowBuilder = rowBuilderFactory.apply(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public Enumerator<T> enumerator() {
        return new ResultSetEnumerator<>(rss, connection, rowBuilderFactory, filterSql);
    }

}
