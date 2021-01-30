package io.mycat.calcite.executor;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import io.mycat.*;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.calcite.Executor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.gsi.GSIService;
import io.mycat.mpp.Row;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.util.FastSqlUtils;
import io.mycat.util.Pair;
import io.mycat.util.SQL;
import io.mycat.util.UpdateSQL;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.apply;

@Getter
public class MycatUpdateExecutor implements Executor {

    private final MycatDataContext context;
    private final MycatUpdateRel mycatUpdateRel;
    private final Distribution distribution;
    /**
     * 逻辑语法树（用户在前端写的SQL语句）
     */
    private final SQLStatement logicStatement;
    /**
     * 逻辑参数 （用户在前端写的SQL语句中的参数）
     */
    private final List<Object> logicParameters;
    /**
     * 由逻辑SQL 改成真正发送给后端数据库的sql语句. 一个不可变的集合 {@link Collections#unmodifiableSet(Set)}
     */
    private final Set<SQL> reallySqlSet;

    private long lastInsertId = 0;
    private long affectedRow = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatUpdateExecutor.class);

    public MycatUpdateExecutor(MycatDataContext context, MycatUpdateRel mycatUpdateRel, Distribution distribution,
                               SQLStatement logicStatement,
                               List<Object> parameters) {
        this.context = context;
        this.mycatUpdateRel = mycatUpdateRel;

        this.distribution = distribution;
        this.logicStatement = logicStatement;
        this.logicParameters = parameters;

        this.reallySqlSet = Collections.unmodifiableSet(buildReallySqlList(mycatUpdateRel,distribution,logicStatement,parameters));
    }

    public static MycatUpdateExecutor create(MycatDataContext context, MycatUpdateRel mycatUpdateRel,Distribution values,
                                             SQLStatement sqlStatement,
                                             List<Object> parameters) {
        return new MycatUpdateExecutor(context,mycatUpdateRel, values, sqlStatement, parameters);
    }

    public static MycatUpdateExecutor create(MycatUpdateRel mycatUpdateRel,MycatDataContext dataContext,List<Object> params) {
        MycatUpdateExecutor updateExecutor;
        if (mycatUpdateRel.isGlobal()) {
            updateExecutor = new MycatGlobalUpdateExecutor(dataContext,mycatUpdateRel, mycatUpdateRel.getValues(),
                    mycatUpdateRel.getSqlStatement(),
                    params);
        } else {
            updateExecutor = MycatUpdateExecutor.create(dataContext,mycatUpdateRel, mycatUpdateRel.getValues(),
                    mycatUpdateRel.getSqlStatement(),
                    params
            );
        }
        return updateExecutor;
    }

    public boolean isProxy() {
        return reallySqlSet.size() == 1;
    }

    public Pair<String, String> getSingleSql() {
        SQL key = reallySqlSet.iterator().next();
        String parameterizedSql = key.getParameterizedSql();
        String sql = apply(parameterizedSql, logicParameters);
        return Pair.of(context.resolveDatasourceTargetName(key.getTarget(),true), sql);
    }

    private FastSqlUtils.Select getSelectPrimaryKeyStatementIfNeed(SQL sql){
        TableHandler table = sql.getTable();
        SQLStatement statement = sql.getStatement();
        if(statement instanceof SQLUpdateStatement) {
            return FastSqlUtils.conversionToSelectSql((SQLUpdateStatement) statement, table.getPrimaryKeyList(),sql.getParameters());
        }else if(statement instanceof SQLDeleteStatement){
            return FastSqlUtils.conversionToSelectSql((SQLDeleteStatement) statement,table.getPrimaryKeyList(),sql.getParameters());
        }
        throw new MycatException("更新语句转查询语句出错，不支持的语法。 \n sql = "+ statement);
    }

    @Override
    @SneakyThrows
    public void open() {
        TransactionSession transactionSession = context.getTransactionSession();
        Map<String, MycatConnection> connections = new HashMap<>(3);
        Set<String> uniqueValues = new HashSet<>();
        for (SQL sql : reallySqlSet) {
            String k = context.resolveDatasourceTargetName(sql.getTarget(),true);
            if (uniqueValues.add(k)) {
                if (connections.put(sql.getTarget(), transactionSession.getJDBCConnection(k)) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }
        }

        SqlRecord sqlRecord = context.currentSqlRecord();
        //建立targetName与连接的映射
        for (SQL sql : reallySqlSet) {
            String parameterizedSql = sql.getParameterizedSql();
            String target = sql.getTarget();

            MycatConnection mycatConnection = connections.get(target);
            Connection connection = mycatConnection.unwrap(Connection.class);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} targetName:{} sql:{} parameters:{} ", mycatConnection, target, parameterizedSql, logicParameters);

            }
            if (LOGGER.isDebugEnabled() && connection.isClosed()) {
                LOGGER.debug("{} has closed but still using", mycatConnection);
            }

            // 如果是更新语法. 例： update set id = 1
            if(sql instanceof UpdateSQL) {
                UpdateSQL updateSQL = (UpdateSQL) sql;
                // 如果用户修改了分片键
                if(updateSQL.isUpdateShardingKey()){
                    onUpdateShardingKey(updateSQL,connection,transactionSession);
                }

                // 如果用户修改了索引
                if(updateSQL.isUpdateIndex()){
                    onUpdateIndex(updateSQL,connection,transactionSession);
                }
            }

            long start = SqlRecord.now();
            SQL.UpdateResult updateResult = sql.executeUpdate(connection);
            Long lastInsertId = updateResult.getLastInsertId();
            int subAffectedRow = updateResult.getAffectedRow();
            sqlRecord.addSubRecord(parameterizedSql,start,SqlRecord.now(),target,subAffectedRow);
            this.affectedRow += subAffectedRow;
            if(lastInsertId != null && lastInsertId > 0) {
                this.lastInsertId = lastInsertId;
            }
        }
    }

    private void onUpdateShardingKey(UpdateSQL<?> sql,Connection connection,TransactionSession transactionSession) throws SQLException {
        List<String> shardingKeys = sql.getSetColumnMap().keySet().stream()
                .filter(SimpleColumnInfo::isShardingKey)
                .map(SimpleColumnInfo::getColumnName)
                .collect(Collectors.toList());
        throw MycatErrorCode.createMycatException(MycatErrorCode.ERR_MODIFY_SHARDING_COLUMN,
                "暂时不支持修改分片键" + shardingKeys);
    }

    private void onUpdateIndex(UpdateSQL<?> sql,Connection connection,TransactionSession transactionSession) throws SQLException {
        if(!MetaClusterCurrent.exist(GSIService.class)){
            return;
        }
        GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
        // 获取主键
        Collection<Map<SimpleColumnInfo, Object>> primaryKeyList;
        if(sql.isWherePrimaryKeyCovering()){
            // 条件满足覆盖主键
            primaryKeyList = sql.getWherePrimaryKeyList();
        }else {
            // 不满足覆盖主键 就查询后端数据库
            primaryKeyList = sql.selectPrimaryKey(connection);
        }

        // 更新索引
        // todo 更新语句包含limit或者order by的情况处理，等实现了全局索引再考虑实现。 wangzihaogithub 2020-12-29
        TableHandler table = sql.getTable();
        gsiService.updateByPrimaryKey(transactionSession.getTxId(),
                table.getSchemaName(),
                table.getTableName(),
                sql.getSetColumnMap(),
                primaryKeyList,sql.getTarget());
    }


    private static Set<SQL> buildReallySqlList(MycatUpdateRel mycatUpdateRel, Distribution distribution, SQLStatement orginalStatement, List<Object> parameters) {
        List<Object> readOnlyParameters = Collections.unmodifiableList(parameters);

        Iterable<DataNode> dataNodes = distribution.getDataNodesAsSingleTableUpdate(mycatUpdateRel.getConditions(),readOnlyParameters);
        Map<SQL,SQL> sqlMap = new LinkedHashMap<>();

        for (DataNode dataNode : dataNodes) {
            SQLStatement currentStatement = FastSqlUtils.clone(orginalStatement);
            SQLExprTableSource tableSource = FastSqlUtils.getTableSource(currentStatement);
            tableSource.setExpr(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            SQL sql = SQL.of(currentStatement.toString(),dataNode, FastSqlUtils.clone(currentStatement),new ArrayList<>(parameters));
            SQL exist = sqlMap.put(sql, sql);
            if(exist != null){
                LOGGER.debug("remove exist sql = {}",exist);
            }
        }
        return new LinkedHashSet<>(sqlMap.keySet());
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
        for (SQL sql : reallySqlSet) {
            String target = sql.getTarget();
            String parameterizedSql = sql.getParameterizedSql();
            explainWriter.item("target:" + target + " " + parameterizedSql, logicParameters);

        }
        return explainWriter.ret();
    }
}