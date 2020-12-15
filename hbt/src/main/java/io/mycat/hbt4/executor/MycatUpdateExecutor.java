package io.mycat.hbt4.executor;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.DataNode;
import io.mycat.MycatConnection;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt4.DataSourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.GroupKey;
import io.mycat.mpp.Row;
import io.mycat.util.Pair;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.apply;
import static java.sql.Statement.NO_GENERATED_KEYS;

@Getter
public class MycatUpdateExecutor implements Executor {
    private final Distribution values;
    private final SQLStatement sqlStatement;
    private List<Object> parameters;
    private final HashSet<GroupKey> groupKeys;
    private DataSourceFactory factory;
    private long lastInsertId = 0;
    private long affectedRow = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatUpdateExecutor.class);

    public MycatUpdateExecutor(Distribution values,
                               SQLStatement sqlStatement,
                               List<Object> parameters,
                               DataSourceFactory factory) {
        this.values = values;
        this.sqlStatement = sqlStatement;
        this.parameters = parameters;
        this.factory = factory;
        this.groupKeys = getGroup();
        factory.registered(this.groupKeys.stream().map(i -> i.getTarget()).distinct().collect(Collectors.toList()));
    }

    public static MycatUpdateExecutor create(Distribution values,
                                             SQLStatement sqlStatement,
                                             DataSourceFactory factory,
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
        String sql = apply(parameterizedSql, parameters);
        return Pair.of(key.getTarget(), sql);
    }

    @Override
    @SneakyThrows
    public void open() {
        Map<String, MycatConnection> connections = factory.getConnections(groupKeys.stream().map(i -> i.getTarget()).distinct().collect(Collectors.toList()));
        boolean insertId = sqlStatement instanceof MySqlInsertStatement;
        for (GroupKey key : groupKeys) {
            String sql = key.getParameterizedSql();
            String target = key.getTarget();
            MycatConnection mycatConnection = connections.get(target);
            Connection connection = mycatConnection.unwrap(Connection.class);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} targetName:{} sql:{} parameters:{} ", mycatConnection, target, sql, parameters);
            }
            if (LOGGER.isDebugEnabled() && connection.isClosed()) {
                LOGGER.debug("{} has closed but still using", mycatConnection);
            }
            PreparedStatement preparedStatement = connection.prepareStatement(sql, insertId ? Statement.RETURN_GENERATED_KEYS : NO_GENERATED_KEYS);
            MycatPreparedStatementUtil.setParams(preparedStatement, parameters);
            this.affectedRow += preparedStatement.executeUpdate();
            this.lastInsertId = Math.max(this.lastInsertId, getInSingleSqlLastInsertId(insertId, preparedStatement));
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
            MycatPreparedStatementUtil.collect(sqlStatement, sb, parameters, outparameters);
            String sql = sb.toString();
            this.parameters = outparameters;
            GroupKey key = GroupKey.of(sql, dataNode.getTargetName());
            groupHashMap.add(key);
        }
        return groupHashMap;
    }

    /**
     * ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
     * 会生成多个值,其中第一个是真正的值
     *
     * @param insertId
     * @param preparedStatement
     * @return
     * @throws SQLException
     */
    public static long getInSingleSqlLastInsertId(boolean insertId, Statement preparedStatement) throws SQLException {
        long lastInsertId = 0;
        if (insertId) {
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys != null) {
                if (generatedKeys.next()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("preparedStatement:{} insertId:{}", preparedStatement, insertId);
                    }
                    long aLong = generatedKeys.getLong(1);
                    lastInsertId = Math.max(lastInsertId, aLong);
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

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        for (GroupKey groupKey : groupKeys) {
            String target = groupKey.getTarget();
            String parameterizedSql = groupKey.getParameterizedSql();
            explainWriter.item("target:" + target + " " + parameterizedSql, parameters);
        }
        return explainWriter.ret();
    }
}