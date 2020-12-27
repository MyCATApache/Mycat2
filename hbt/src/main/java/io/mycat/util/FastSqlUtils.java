package io.mycat.util;

import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLSelect;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.SimpleColumnInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FastSqlUtils {

    public static Select conversionToSelectSql(MySqlDeleteStatement updateStatement, List<SimpleColumnInfo> selectColumnList,List<Object> updateParameters){
        MySqlSelectQueryBlock sqlSelectQueryBlock = new MySqlSelectQueryBlock();
        sqlSelectQueryBlock.setWhere(updateStatement.getWhere());
        sqlSelectQueryBlock.setLimit(updateStatement.getLimit());
        sqlSelectQueryBlock.setFrom(updateStatement.getTableSource());
        sqlSelectQueryBlock.setOrderBy(updateStatement.getOrderBy());

        List<SQLSelectItem> selectList = selectColumnList.stream()
                .map(e -> new SQLSelectItem(new SQLIdentifierExpr(e.getColumnName())))
                .collect(Collectors.toList());
        sqlSelectQueryBlock.getSelectList().addAll(selectList);

        SQLSelect sqlSelect = new SQLSelect(sqlSelectQueryBlock);
        sqlSelect.setWithSubQuery(updateStatement.getWith());
        SQLSelectStatement statement = new SQLSelectStatement(sqlSelect, updateStatement.getDbType());
        return new Select(statement,updateParameters.subList(0,updateParameters.size()));
    }

    public static Select conversionToSelectSql(MySqlUpdateStatement updateStatement, List<SimpleColumnInfo> selectColumnList,List<Object> updateParameters){
        MySqlSelectQueryBlock sqlSelectQueryBlock = new MySqlSelectQueryBlock();
        sqlSelectQueryBlock.setWhere(updateStatement.getWhere());
        sqlSelectQueryBlock.setLimit(updateStatement.getLimit());
        sqlSelectQueryBlock.setFrom(updateStatement.getTableSource());
        sqlSelectQueryBlock.setOrderBy(updateStatement.getOrderBy());

        List<SQLSelectItem> selectList = selectColumnList.stream()
                .map(e -> new SQLSelectItem(new SQLIdentifierExpr(e.getColumnName())))
                .collect(Collectors.toList());
        sqlSelectQueryBlock.getSelectList().addAll(selectList);

        SQLSelect sqlSelect = new SQLSelect(sqlSelectQueryBlock);
        sqlSelect.setWithSubQuery(updateStatement.getWith());
        SQLSelectStatement statement = new SQLSelectStatement(sqlSelect, updateStatement.getDbType());
        long count = updateStatement.getItems().stream()
                .filter(e -> "?".equals(Objects.toString(e.getValue())))
                .count();
        return new Select(statement,updateParameters.subList((int) count,updateParameters.size()));
    }

    @AllArgsConstructor
    @Getter
    public static class Select{
        private SQLSelectStatement statement;
        private List<Object> parameters;
    }
}
