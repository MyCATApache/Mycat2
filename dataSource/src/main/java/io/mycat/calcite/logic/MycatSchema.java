package io.mycat.calcite.logic;

import lombok.AllArgsConstructor;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
public class MycatSchema extends AbstractSchema {
    private final  Map<String,Table> tableMap = new ConcurrentHashMap<>();;
    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }
}