package io.mycat.mpp.plan;

import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DataAccessorImpl implements DataAccessor {
    final Object[] row;

    public DataAccessorImpl(Object[] row) {
        this.row = row;
    }

    @Override
    public Object get(int index) {
        return row[index];
    }

    @Override
    public DataAccessor map(Object[] row) {
        return new DataAccessorImpl(row);
    }

    @Override
    public String toString() {
        List<String> collect = Arrays.asList(row).stream()
                .map(i -> SQLExprUtils.fromJavaObject(i).toString()).collect(Collectors.toList());
        return Arrays.toString(collect.toArray());
    }
}