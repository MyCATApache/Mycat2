package io.mycat.calcite;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;

import java.sql.Connection;
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

    public MyCatResultSetEnumerable(Connection connection, Function1<ResultSet, Function0<T>> rowBuilderFactory) {
        this.connection = connection;
        this.rowBuilderFactory = rowBuilderFactory;

        try {
            System.out.println("run query");
            rss = new ArrayList<>();
            rs = connection.createStatement().executeQuery("select * from test.test");
            rss.add(rs);
            rss.add(connection.createStatement().executeQuery("select * from test.test1"));

            rowBuilder = rowBuilderFactory.apply(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public Enumerator<T> enumerator() {
        return new ResultSetEnumerator<>(rss, connection, rowBuilderFactory);
    }

}
