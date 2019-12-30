package cn.lightfish.rsqlBuilder.schema;


import cn.lightfish.describer.ParseNode;
import cn.lightfish.describer.ParseNodeVisitor;
import cn.lightfish.rsqlBuilder.DotAble;

import java.util.Map;

public class SchemaObject implements DotAble, ParseNode {
    private final String schema;
    private final Map<String, Map<String, ParseNode>> tables;

    public SchemaObject(String string, Map<String, Map<String, ParseNode>> stringMapMap) {
        this.schema = string;
        this.tables = stringMapMap;
    }


    public TableObject dotAttribute(String o) {
        o = o.toLowerCase();
        Map<String, ParseNode> map = tables.get(o);
        return new TableObject(schema, o, map);
    }

    @Override
    public String toString() {
        return "SchemaObject{" + schema + "}";
    }

    @Override
    public <T> T dot(String o) {
        return (T) dotAttribute(o);
    }


    @Override
    public void accept(ParseNodeVisitor visitor) {

    }

    @Override
    public SchemaObject copy() {
        return this;
    }
}