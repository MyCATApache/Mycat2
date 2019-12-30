package cn.lightfish.rsqlBuilder.schema;

import cn.lightfish.describer.ParseNode;
import cn.lightfish.describer.ParseNodeVisitor;
import cn.lightfish.rsqlBuilder.DotAble;

import java.util.Map;

public class TableObject implements DotAble, DataSource {
    private final String schema;
    private final String tableName;
    private final Map<String, ParseNode> map;

    public TableObject(String schema, String o, Map<String, ParseNode> map) {
        this.schema = schema;
        this.tableName = o;
        this.map = map;
    }


    @Override
    public String toString() {
        return "TableObject{" +
                schema + "." + tableName + "}";
    }

    @Override
    public ColumnObject getColumn(String columnName) {
        columnName = columnName.toLowerCase();
        if (map.containsKey(columnName)) {
            return new ColumnObject(schema, tableName, columnName);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public ColumnObject dot(String o) {
        return getColumn(o);
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {

    }

    @Override
    public TableObject copy() {
        return this;
    }
}