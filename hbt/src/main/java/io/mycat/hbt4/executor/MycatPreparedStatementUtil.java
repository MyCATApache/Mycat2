package io.mycat.hbt4.executor;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import io.mycat.MycatConnection;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt4.Group;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class MycatPreparedStatementUtil {
     public static void collect(SQLObject sqlObject,List<Object> inParams,List<Object> outParams){
          sqlObject.accept(new MySqlASTVisitorAdapter(){
             @Override
             public boolean visit(SQLVariantRefExpr x) {
                 int index = x.getIndex();
                 if (index >=0) {
                     outParams.add(inParams.get(index));
                 }
                 return super.visit(x);
             }
         });
     }
    public static ExecuteBatchInsert batchInsert(String sql, Group value, Connection connection) {
        ExecuteBatchInsert executeBatchInsert = new ExecuteBatchInsert(sql, value, connection);
        return executeBatchInsert.invoke();
    }

    public static class ExecuteBatchInsert {
        private long lastInsertId;
        private long affected;
        private String sql;
        private Group value;
        private Connection connection;

        public ExecuteBatchInsert(String sql, Group value, Connection connection) {
            this.lastInsertId = 0;
            this.affected = 0;
            this.sql = sql;
            this.value = value;
            this.connection = connection;
        }

        public long getLastInsertId() {
            return lastInsertId;
        }

        public long getAffected() {
            return affected;
        }

        @SneakyThrows
        public ExecuteBatchInsert invoke() {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                for (List<Object> objects : value.args) {
                    setParams(preparedStatement, objects);
                    preparedStatement.addBatch();
                }
                int[] ints = preparedStatement.executeBatch();
                for (int anInt : ints) {
                    affected += anInt;
                }
                ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    lastInsertId = Math.max(lastInsertId, generatedKeys.getLong(1));
                }
            }
            return this;
        }
    }

    public static void setParams(PreparedStatement preparedStatement, List<Object> objects) throws SQLException {
        int index = 1;
        for (Object object : objects) {
            preparedStatement.setObject(index, object);
            index++;
        }
    }

    public static String apply(String parameterizedSql,List<Object> parameters) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedSql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                parent.replace(x, SQLExprUtils.fromJavaObject(parameters.get(x.getIndex())));
            }
        });
        return sqlStatement.toString();
    }
    @SneakyThrows
    public static RowBaseIterator executeQuery(Connection mycatConnection,
                                               MycatRowMetaData calciteRowMetaData,
                                               SqlString value,
                                               List<Object> params) {
        String sql = value.getSql();
        PreparedStatement preparedStatement = mycatConnection.prepareStatement(sql);
        ImmutableList<Integer> dynamicParameters = value.getDynamicParameters();
        if (dynamicParameters != null && !dynamicParameters.isEmpty()) {
            MycatPreparedStatementUtil.setParams(preparedStatement, dynamicParameters.stream().map(i -> params.get(i)).collect(Collectors.toList()));
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        return new JdbcRowBaseIterator(calciteRowMetaData, preparedStatement, resultSet, null, sql);
    }

}