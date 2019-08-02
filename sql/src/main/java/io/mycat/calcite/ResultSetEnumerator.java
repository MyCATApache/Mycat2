package io.mycat.calcite;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetEnumerator<T> implements Enumerator<T> {
    private List<ResultSet> rss;
    private Connection connection;
    private Function1<ResultSet, Function0<T>> rowBuilderFactory;
    private  List<Function0<T>> rowBuilders;
    private int size = 0;
    private int currentChannel = 0;
    private ResultSet currentrs;

    private  String filterSql;
    public ResultSetEnumerator(List<ResultSet> rss, Connection connection,
                               Function1<ResultSet, Function0<T>> rowBuilderFactory, String filterSql) {
        this.connection = connection;
        this.rss = rss;
        this.size = rss.size();
        rowBuilders = new ArrayList<Function0<T>>();
        for (ResultSet rs : rss) {
            rowBuilders.add(rowBuilderFactory.apply(rs));
        }

        this.rowBuilderFactory = rowBuilderFactory;
        this.filterSql = filterSql;
    }
    @Override
    public T current() {
        // return rowBuilders.get(currentChannel - 1).apply();
        return rowBuilderFactory.apply(currentrs).apply();
    }

    @Override
    public boolean moveNext() {
        boolean result = false;
        try {
            while (!rss.isEmpty()) {
                currentrs = rss.get(0);
                result = currentrs.next();
                if (result == true) {
                    return result;
                }
                rss.remove(0);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("aa");
        }
        return result;
    }

    @Override
    public void reset() {
        try {
            for(ResultSet rs :rss ) {
                rs.beforeFirst();
            }
        } catch (SQLException e) {
            throw new RuntimeException("aaa");
        }
    }

    @Override
    public void close() {
        try {
            for (ResultSet rs :rss){
                rs.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("can't close");
        }
    }
}