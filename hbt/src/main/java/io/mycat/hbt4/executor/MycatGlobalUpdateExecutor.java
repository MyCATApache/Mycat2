package io.mycat.hbt4.executor;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt4.DataSourceFactory;

import java.util.List;

public class MycatGlobalUpdateExecutor extends MycatUpdateExecutor {
    public MycatGlobalUpdateExecutor(Distribution values, SQLStatement sqlStatement, List<Object> parameters, DataSourceFactory factory) {
        super(values, sqlStatement, parameters, factory);
    }

    @Override
    public long getAffectedRow() {
        return super.getAffectedRow()/this.getGroupKeys().size();
    }
}
