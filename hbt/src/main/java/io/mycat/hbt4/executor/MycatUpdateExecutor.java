package io.mycat.hbt4.executor;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.DataNode;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt4.DatasourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.GroupKey;
import io.mycat.mpp.Row;
import io.mycat.util.Pair;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.apply;
import static java.sql.Statement.NO_GENERATED_KEYS;

@Getter
public class MycatUpdateExecutor implements Executor {
    private final Distribution values;
    private final SQLStatement sqlStatement;
    private  List<Object> parameters;
    private final HashSet<GroupKey> groupKeys;
    private DatasourceFactory factory;
    public long lastInsertId = 0;
    public long affectedRow = 0;

    public MycatUpdateExecutor(Distribution values,
                               SQLStatement sqlStatement,
                               List<Object> parameters,
                               DatasourceFactory factory) {
        this.values = values;
        this.sqlStatement = sqlStatement;
        this.parameters = parameters;
        this.factory = factory;
        this.groupKeys = getGroup();
    }

    public static MycatUpdateExecutor create(Distribution values,
                                             SQLStatement sqlStatement,
                                             DatasourceFactory factory,
                                             List<Object> parameters) {
        return new MycatUpdateExecutor(values, sqlStatement, parameters, factory);
    }

    public boolean isProxy() {
        return groupKeys.size() == 1;
    }

    public Pair<String, String> getSingleSql() {
        GroupKey groupKey = groupKeys.iterator().next();
        GroupKey key = groupKey;
        String parameterizedSql = key.getParameterizedSql();
        String sql = apply(parameterizedSql,parameters);
        return Pair.of(key.getTarget(), sql);
    }

    @Override
    @SneakyThrows
    public void open() {
        Map<String, Connection> connections = factory.getConnections(groupKeys.stream().map(i -> i.getTarget()).distinct().collect(Collectors.toList()));
        boolean insertId = sqlStatement instanceof MySqlInsertStatement;
        for (GroupKey key : groupKeys) {
            String sql = key.getParameterizedSql();
            String target = key.getTarget();
            Connection connection = connections.get(target);
            PreparedStatement preparedStatement = connection.prepareStatement(sql, insertId ? Statement.RETURN_GENERATED_KEYS : NO_GENERATED_KEYS);
            MycatPreparedStatementUtil.setParams(preparedStatement, parameters);
            this.affectedRow += preparedStatement.executeUpdate();
            this.lastInsertId = Math.max(this.lastInsertId, getLastInsertId(insertId, preparedStatement));
        }
    }

    @NotNull
    private HashSet<GroupKey> getGroup() {
        Iterable<DataNode> dataNodes = values.getDataNodes(parameters);
        HashSet<GroupKey> groupHashMap = new HashSet<>();
        for (DataNode dataNode : dataNodes) {
            SQLExprTableSource tableSource = null;
            if (sqlStatement instanceof MySqlUpdateStatement) {
                tableSource = (SQLExprTableSource) ((MySqlUpdateStatement) sqlStatement).getTableSource();
            }
            if (sqlStatement instanceof MySqlDeleteStatement) {
                tableSource = (SQLExprTableSource) ((MySqlDeleteStatement) sqlStatement).getTableSource();
            }
            if (sqlStatement instanceof MySqlInsertStatement) {
                tableSource = (SQLExprTableSource) ((MySqlInsertStatement) sqlStatement).getTableSource();
            }
            Objects.requireNonNull(tableSource);
            tableSource.setExpr(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            StringBuilder sb = new StringBuilder();
            List<Object> outparameters = new ArrayList<>();
             MycatPreparedStatementUtil.collect(sqlStatement,sb,parameters,outparameters);
            String sql =sb.toString();
            this.parameters = outparameters;
            GroupKey key = GroupKey.of(sql, dataNode.getTargetName());
            groupHashMap.add(key);
        }
        return groupHashMap;
    }

    public static long getLastInsertId(boolean insertId, Statement preparedStatement) throws SQLException {
        long lastInsertId = 0;
        if (insertId) {
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys != null) {
                if (generatedKeys.next()) {
                    lastInsertId = generatedKeys.getLong(1);
                }
            }
        }
        return lastInsertId;
    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}