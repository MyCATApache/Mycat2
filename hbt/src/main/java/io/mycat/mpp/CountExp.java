package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import io.mycat.mpp.plan.DataAccessor;

import java.util.List;

public class CountExp extends AggregationExp {
    public CountExp(List<SqlValue> param,String columnName) {
        super("count", param, columnName);
        reset();
    }

    private long count;

    @Override
    public void accept(DataAccessor tuple) {
    }

    @Override
  public   Object getValue() {
        return count;
    }

    @Override
    public void reset() {
        this.count = 0;
    }

    @Override
    public Class type() {
        return Long.TYPE;
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }
}