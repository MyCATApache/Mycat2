package io.mycat.route;

import io.mycat.hbt.ast.base.OrderItem;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.query.FromSqlSchema;

import java.util.Collections;
import java.util.List;

public class HBTBuilder {
    Schema schema;
    public static HBTBuilder create() {
        return new HBTBuilder();
    }

    public  HBTBuilder  from(String targetName, String sql) {
        schema = new FromSqlSchema(Collections.emptyList(),targetName,sql);
        return this;
    }

    public HBTBuilder distinct() {
        return null;
    }

    public HBTBuilder union(boolean b) {
        return null;
    }

    public HBTBuilder order(List<OrderItem> concertOrder) {
        return null;
    }

    public HBTBuilder limit(List<OrderItem> concertOrder) {
        return null;
    }

    public Schema build() {
        return schema;
    }
}