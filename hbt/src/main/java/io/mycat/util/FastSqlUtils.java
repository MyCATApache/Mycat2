package io.mycat.util;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.MycatException;
import io.mycat.SimpleColumnInfo;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 封装统一了一些 com.alibaba.fastsql的常用方法
 * @author wangzihaogithub 2020-12-29
 */
@Slf4j
public class FastSqlUtils {

    public static <T extends SQLObject>T clone(T source){
        T target = (T) source.clone();
        if(source.getClass() != target.getClass()){
            target = (T) SQLUtils.parseSingleMysqlStatement(source.toString());
        }
        if(log.isDebugEnabled() &&
                !Objects.equals(
                        source.toString().replace("\t","").replace(" ","").replace("\n",""),
                        target.toString().replace("\t","").replace(" ","").replace("\n",""))) {
            throw new MycatException("mycat内部异常， clone对象不一致：source = " + source + "， target" + target);
        }
        return target;
   }

    public static SQLExprTableSource getTableSource(SQLStatement sqlStatement){
        SQLExprTableSource tableSource;
        if (sqlStatement instanceof SQLUpdateStatement) {
            tableSource = (SQLExprTableSource) ((SQLUpdateStatement) sqlStatement).getTableSource();
        }else if (sqlStatement instanceof SQLDeleteStatement) {
            tableSource = (SQLExprTableSource) ((SQLDeleteStatement) sqlStatement).getTableSource();
        }else if (sqlStatement instanceof SQLInsertInto) {
            tableSource = ((SQLInsertInto) sqlStatement).getTableSource();
        }else {
            throw new IllegalStateException("not found tableSource. sql = "+sqlStatement);
        }
        return tableSource;
    }

    public static Select conversionToSelectSql(SQLDeleteStatement updateStatement, List<SimpleColumnInfo> selectColumnList,List<Object> updateParameters){
        MySqlSelectQueryBlock sqlSelectQueryBlock = new MySqlSelectQueryBlock();
        if(updateStatement instanceof MySqlDeleteStatement){
            sqlSelectQueryBlock.setLimit(((MySqlDeleteStatement) updateStatement).getLimit());
            sqlSelectQueryBlock.setOrderBy(((MySqlDeleteStatement) updateStatement).getOrderBy());
        }
        sqlSelectQueryBlock.setWhere(updateStatement.getWhere());
        sqlSelectQueryBlock.setFrom(updateStatement.getTableSource());
        List<SQLSelectItem> selectList = selectColumnList.stream()
                .map(e -> new SQLSelectItem(new SQLIdentifierExpr(e.getColumnName())))
                .collect(Collectors.toList());
        sqlSelectQueryBlock.getSelectList().addAll(selectList);

        SQLSelect sqlSelect = new SQLSelect(sqlSelectQueryBlock);
        sqlSelect.setWithSubQuery(updateStatement.getWith());
        SQLSelectStatement statement = new SQLSelectStatement(sqlSelect, updateStatement.getDbType());
        return new Select(statement,updateParameters.subList(0,updateParameters.size()),selectColumnList);
    }

    public static Select conversionToSelectSql(SQLUpdateStatement updateStatement, List<SimpleColumnInfo> selectColumnList,List<Object> updateParameters){
        SQLSelectQueryBlock sqlSelectQueryBlock = new MySqlSelectQueryBlock();
        if(updateStatement instanceof MySqlUpdateStatement){
            sqlSelectQueryBlock.setLimit(((MySqlUpdateStatement) updateStatement).getLimit());
        }
        sqlSelectQueryBlock.setWhere(updateStatement.getWhere());
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
        return new Select(statement,updateParameters.subList((int) count,updateParameters.size()),selectColumnList);
    }

    @AllArgsConstructor
    @Getter
    public static class Select{
        private final SQLSelectStatement statement;
        private final List<Object> parameters;
        private final List<SimpleColumnInfo> selectColumnList;

        public Collection<Map<SimpleColumnInfo,Object>> executeQuery(Connection connection) throws SQLException {
            PreparedStatement preparedStatement = MycatPreparedStatementUtil.setParams(
                    connection.prepareStatement(statement.toString()),parameters);
            ResultSet resultSet = preparedStatement.executeQuery();
            int initialCapacity = (int) Math.max(selectColumnList.size() * 0.75F, 1);
            Iterable<Map<SimpleColumnInfo,Object>> iterable = () -> new Iterator<Map<SimpleColumnInfo, Object>>() {
                @SneakyThrows
                @Override
                public boolean hasNext() {
                    boolean next = resultSet.next();
                    if(!next){
                        resultSet.close();
                        resultSet.getStatement().close();
                    }
                    return next;
                }

                @SneakyThrows
                @Override
                public Map<SimpleColumnInfo, Object> next() {
                    Map<SimpleColumnInfo, Object> map = new LinkedHashMap<>(initialCapacity, 0.75F);
                    for (int i = 1; i <= selectColumnList.size(); i++) {
                        SimpleColumnInfo columnInfo = selectColumnList.get(i - 1);
                        Object value = resultSet.getObject(i);
                        map.put(columnInfo, value);
                    }
                    return map;
                }
            };
            return LazyTransformCollection.transform(iterable);
        }

        @Override
        public String toString() {
            return statement.toString();
        }
    }
}
