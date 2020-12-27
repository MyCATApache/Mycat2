package io.mycat.calcite.executor;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;
import io.mycat.calcite.DataSourceFactory;
import io.mycat.calcite.Executor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.hbt.TextConvertor;
import io.mycat.mpp.Row;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.util.FastSqlUtils;
import io.mycat.util.Pair;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.apply;
import static java.sql.Statement.NO_GENERATED_KEYS;

@Getter
public class MycatUpdateExecutor implements Executor {
    private MycatDataContext context;
    private final Distribution values;
    private final SQLStatement sqlStatement;
    private List<Object> parameters;
    private final HashSet<GroupKey> groupKeys;
    private DataSourceFactory factory;
    private long lastInsertId = 0;
    private long affectedRow = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatUpdateExecutor.class);

    public MycatUpdateExecutor(MycatDataContext context, Distribution values,
                               SQLStatement sqlStatement,
                               List<Object> parameters,
                               DataSourceFactory factory) {
        this.context = context;
        this.values = values;
        this.sqlStatement = sqlStatement;
        this.parameters = parameters;
        this.factory = factory;
        this.groupKeys = getGroup();
        factory.registered(this.groupKeys.stream().map(i -> i.getTarget()).distinct().collect(Collectors.toList()));
    }

    public static MycatUpdateExecutor create(MycatDataContext context, Distribution values,
                                             SQLStatement sqlStatement,
                                             DataSourceFactory factory,
                                             List<Object> parameters) {
        return new MycatUpdateExecutor(context,values, sqlStatement, parameters, factory);
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


    private FastSqlUtils.Select getSelectPrimaryKeyStatementIfNeed(DataNode dataNode, MetadataManager metadataManager){
        TableHandler table = metadataManager.getTable(dataNode.getSchema(), dataNode.getTable());
        if(sqlStatement instanceof MySqlUpdateStatement) {
            return FastSqlUtils.conversionToSelectSql((MySqlUpdateStatement) sqlStatement, table.getPrimaryKeyList(),parameters);
        }else if(sqlStatement instanceof MySqlDeleteStatement){
            return FastSqlUtils.conversionToSelectSql((MySqlDeleteStatement) sqlStatement,table.getPrimaryKeyList(),parameters);
        }
        return null;
    }

    @Override
    @SneakyThrows
    public void open() {

        TransactionSession transactionSession = context.getTransactionSession();

        Map<String, MycatConnection> connections = new HashMap<>();
        Set<String> uniqueValues = new HashSet<>();
        for (GroupKey target : groupKeys) {
            String k = context.resolveDatasourceTargetName(target.getTarget());
            if (uniqueValues.add(k)) {
                if (connections.put(target.getTarget(), transactionSession.getConnection(k)) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }
        }


        boolean insertId = sqlStatement instanceof MySqlInsertStatement;
        SqlRecord sqlRecord = context.currentSqlRecord();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        //建立targetName与连接的映射
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

            FastSqlUtils.Select select = getSelectPrimaryKeyStatementIfNeed(key.getDataNode(), metadataManager);
            if(select != null) {
                try(PreparedStatement statement = MycatPreparedStatementUtil.setParams(
                        connection.prepareStatement(select.getStatement().toString()),select.getParameters());
                    ResultSet resultSet = statement.executeQuery()){
                    String s = TextConvertor.dumpResultSet(resultSet);
                    System.out.println("selectPrimaryKeyStatement = " + s);
                }
            }

            long start = SqlRecord.now();
            PreparedStatement preparedStatement = connection.prepareStatement(sql, insertId ? Statement.RETURN_GENERATED_KEYS : NO_GENERATED_KEYS);
            MycatPreparedStatementUtil.setParams(preparedStatement, parameters);
            int subAffectedRow = preparedStatement.executeUpdate();
            sqlRecord.addSubRecord(sql,start,SqlRecord.now(),target,subAffectedRow);
            this.affectedRow += subAffectedRow;
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
            GroupKey key = GroupKey.of(sql, dataNode.getTargetName(),dataNode);
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