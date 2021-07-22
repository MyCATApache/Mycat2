package io.mycat.router.function;

import io.mycat.BackendTableInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class IndexDataNode extends BackendTableInfo {

    private final int dbIndex;
    private final int tableIndex;
    private final int index;

    public IndexDataNode(String targetName, String targetSchema, String targetTable,
                         int index,
                         int dbIndex,
                         int tableIndex) {
        super(targetName, targetSchema, targetTable);
        this.dbIndex = dbIndex;
        this.tableIndex = tableIndex;
        this.index = index;
    }

    @Override
    public String toString() {
        return "{" +
                "targetName='" + getTargetName() + '\'' +
                ", schemaName='" + getSchema() + '\'' +
                ", tableName='" + getTable() + '\'' +
                ", index=" + index +
                ", dbIndex=" + dbIndex +
                ", tableIndex=" + tableIndex +
                '}';
    }

    @Override
    public Integer getDbIndex() {
        return dbIndex;
    }

    @Override
    public Integer getTableIndex() {
        return tableIndex;
    }

    @Override
    public Integer getIndex() {
        return index;
    }
}