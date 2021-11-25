package io.mycat;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.config.ViewConfig;

import java.util.List;
import java.util.stream.Collectors;

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

    @Override
    public ViewConfig getConfig() {
        ViewConfig config = new ViewConfig();
        SQLCreateViewStatement statement = new SQLCreateViewStatement();
        List<SQLColumnDefinition> columnDefinitions = columns.stream().map(c -> {
            SQLColumnDefinition column = new SQLColumnDefinition();
            column.setDbType(DbType.mysql);
            column.setName(c);
            return column;
        }).collect(Collectors.toList());
        SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();
        sqlExprTableSource.setSchema(getSchemaName());
        sqlExprTableSource.setSimpleName(getViewName());

        statement.setTableSource(sqlExprTableSource);
        columnDefinitions.forEach(c->statement.addColumn(c));
        statement.setSubQuery(((SQLSelectStatement)SQLUtils.parseSingleMysqlStatement(viewSql)).getSelect());

        config.setCreateViewSQL(statement.toString());
        return config;
    }
}
