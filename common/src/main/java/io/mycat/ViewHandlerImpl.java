package io.mycat;

import java.util.List;

public class ViewHandlerImpl implements ViewHandler {
    final String schemaName;
    final String viewName;
    final List<String> columns;
    final String viewSql;

    public ViewHandlerImpl(String schemaName, String viewName, List<String> columns, String viewSql) {
        this.schemaName = schemaName;
        this.viewName = viewName;
        this.columns = columns;
        this.viewSql = viewSql;
    }

    public static ViewHandler create(String schemaName, String viewName, List<String> columns, String viewSql) {
        return new ViewHandlerImpl(schemaName, viewName, columns, viewSql);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getViewName() {
        return viewName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getViewSql() {
        return viewSql;
    }
}
