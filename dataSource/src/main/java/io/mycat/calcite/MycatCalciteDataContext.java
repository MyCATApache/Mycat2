package io.mycat.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;

import java.util.Map;


public class MycatCalciteDataContext implements DataContext {
    private final SchemaPlus rootSchema;
    private final Planner planner;
    private final Map<String,Object> variables;

    public MycatCalciteDataContext(SchemaPlus rootSchema, Planner planner) {
        this.rootSchema = rootSchema;
        this.planner = planner;
        this.variables = null;
    }

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
        if (variables==null){
            return null;
        }
        return variables.get(name);
    }
}