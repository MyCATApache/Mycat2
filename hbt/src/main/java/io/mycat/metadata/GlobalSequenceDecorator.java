package io.mycat.metadata;

import io.mycat.hbt.TextUpdateInfo;
import io.mycat.queryCondition.SimpleColumnInfo;
import lombok.AllArgsConstructor;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

@AllArgsConstructor
public class GlobalSequenceDecorator implements TableHandler {
    final TableHandler handler;
    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> insertHandler() {
        return handler.insertHandler();
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> updateHandler() {
        return handler.updateHandler();
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> deleteHandler() {
        return handler.deleteHandler();
    }

    @Override
    public LogicTableType getType() {
        return handler.getType();
    }

    @Override
    public String getSchemaName() {
        return handler.getSchemaName();
    }

    @Override
    public String getTableName() {
        return handler.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return handler.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getRawColumns() {
        return handler.getRawColumns();
    }
}