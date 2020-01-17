package io.mycat.calcite;

import lombok.AllArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;

@AllArgsConstructor
public class CalciteDataContext implements DataContext {
    private final SchemaPlus rootSchema;
    private final Planner planner;

    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    public JavaTypeFactory getTypeFactory() {
        return (JavaTypeFactory) planner.getTypeFactory();
    }

    public QueryProvider getQueryProvider() {
        return null;
    }

    public Object get(String name) {
        return null;
    }
}