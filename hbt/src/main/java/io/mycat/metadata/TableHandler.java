package io.mycat.metadata;

import io.mycat.hbt.TextUpdateInfo;
import io.mycat.queryCondition.SimpleColumnInfo;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface TableHandler {
    public Function<ParseContext, Iterator<TextUpdateInfo>> insertHandler();

    public Function<ParseContext, Iterator<TextUpdateInfo>> updateHandler();

    public Function<ParseContext, Iterator<TextUpdateInfo>> deleteHandler();

    public LogicTableType getType();

    String getSchemaName();

    String getTableName();

    String getCreateTableSQL();

    List<SimpleColumnInfo> getRawColumns();
}