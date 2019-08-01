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
    public ResultSetEnumerator(List<ResultSet> rss, Connection connection,
                               Function1<ResultSet, Function0<T>> rowBuilderFactory) {
        this.connection = connection;
        this.rss = rss;
        this.size = rss.size();
        rowBuilders = new ArrayList<Function0<T>>();
        for (ResultSet rs : rss) {
            rowBuilders.add(rowBuilderFactory.apply(rs));
        }

    }
    @Override
    public T current() {
        return rowBuilders.get(currentChannel - 1).apply();
    }

    @Override
    public boolean moveNext() {
        boolean result;
        try {
            if (currentChannel >= size ) {
                currentChannel = 0;
            }
            // TODO:check all currentChannel
            result = rss.get(currentChannel).next();
            currentChannel++;

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