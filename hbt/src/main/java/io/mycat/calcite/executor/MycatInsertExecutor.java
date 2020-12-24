package io.mycat.calcite.executor;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLNullExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.visitor.SQLEvalVisitorUtils;
import io.mycat.*;
import io.mycat.calcite.DataSourceFactory;
import io.mycat.calcite.Executor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.gsi.GSIService;
import io.mycat.mpp.Row;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.util.Pair;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.MycatContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.setParams;

@Getter
public class MycatInsertExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatInsertExecutor.class);

    private MycatDataContext context;
    private final MycatInsertRel mycatInsertRel;
    private final DataSourceFactory factory;
    /**
     * 最终发给后端的sql, 包含全部字段的数据 （比如自增ID）
     */
    private final Map<GroupKey, Group> groupMap;
    /**
     * 只包含sql中明确写的字段 （不包含自增ID 等其他隐式变量）
     */
    private List<Object> params;
    /**
     * 用于区分是否批量插入
     * multi = true 表示 insert into t_user (id,name) values (1,'李'), (2,'王');
     * multi = false 表示 insert into t_user (id,name) values (1,'李');
     */
    private final boolean multi;
    public long lastInsertId = 0;
    public long affectedRow = 0;
    public String sequence;
    private boolean done = false;

    public MycatInsertExecutor(MycatDataContext context, MycatInsertRel mycatInsertRel, DataSourceFactory factory, List<Object> params) {
        this.context = context;
        this.mycatInsertRel = mycatInsertRel;
        this.factory = factory;
        this.params = params;

        this.multi = !params.isEmpty() && (params.get(0) instanceof List);
        if (multi) {
            this.groupMap = runMultiParams();
        } else {
            this.groupMap = runNormalParams();
        }
        this.factory.registered(this.groupMap.keySet().stream().map(i -> i.getTarget()).distinct().collect(Collectors.toList()));
    }

    public boolean isProxy() {
        return params.isEmpty() && mycatInsertRel.getFinalAutoIncrementIndex() != -1 && groupMap.keySet().size() == 1;
    }

    public Pair<String, String> getSingleSql() {
        Map.Entry<GroupKey, Group> entry = groupMap.entrySet().iterator().next();
        GroupKey key = entry.getKey();
        String parameterizedSql = key.getParameterizedSql();
        LinkedList<List<Object>> args = entry.getValue().getArgs();
        if (args.isEmpty() && mycatInsertRel.getFinalAutoIncrementIndex() != -1) {
            return Pair.of(key.getTarget(), parameterizedSql);
        }
        MySqlInsertStatement sqlStatement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(parameterizedSql);
        Group value = entry.getValue();
        List<SQLInsertStatement.ValuesClause> valuesList = sqlStatement.getValuesList();
        SQLInsertStatement.ValuesClause values = sqlStatement.getValues();
        valuesList.clear();
        for (List<Object> arg : value.args) {
            SQLInsertStatement.ValuesClause valuesClause = values.clone();
            valuesClause.accept(new MySqlASTVisitorAdapter() {
                @Override
                public void endVisit(SQLVariantRefExpr x) {
                    SQLReplaceable parent = (SQLReplaceable) x.getParent();
                    parent.replace(x, SQLExprUtils.fromJavaObject(arg.get(x.getIndex())));
                }
            });
            valuesList.add(valuesClause);
        }
        return Pair.of(key.getTarget(), sqlStatement.toString());
    }

    @NotNull
    @Override
    public Iterator<Object[]> outputObjectIterator() {
        return null;
    }

    public static MycatInsertExecutor create(MycatDataContext context, MycatInsertRel mycatInsertRel, DataSourceFactory factory, List<Object> params) {
        return new MycatInsertExecutor(context, mycatInsertRel, factory, params);
    }

    @Override
    public void open() {
        try {
            execute(groupMap);
            onInsertSuccess();
        }finally {
            done = true;
        }
    }

    @SneakyThrows
    private Map<GroupKey, Group> runNormalParams() {
        MySqlInsertStatement mySqlInsertStatement = (MySqlInsertStatement) mycatInsertRel.getMySqlInsertStatement();
        ShardingTableHandler logicTable = mycatInsertRel.getLogicTable();
        CustomRuleFunction function = logicTable.function();
        int finalAutoIncrementIndex = mycatInsertRel.getFinalAutoIncrementIndex();
        List<Integer> shardingKeys = mycatInsertRel.getShardingKeys();
        String[] columnNames = mycatInsertRel.getColumnNames();
        Supplier<Number> stringSupplier = logicTable.nextSequence();

        MySqlInsertStatement template = (MySqlInsertStatement) mySqlInsertStatement.clone();
        List<SQLInsertStatement.ValuesClause> valuesList = template.getValuesList();
        valuesList.clear();

        Map<GroupKey, Group> group = new HashMap<>();
        for (SQLInsertStatement.ValuesClause valuesClause : mySqlInsertStatement.getValuesList()) {
            boolean fillSequence = finalAutoIncrementIndex == -1 && logicTable.isAutoIncrement();
            Number sequence = null;
            if (fillSequence) {
                sequence = stringSupplier.get();
                valuesClause.addValue(SQLExprUtils.fromJavaObject(sequence));
            }
            Map<String, List<RangeVariable>> variables = compute(shardingKeys, columnNames, valuesClause.getValues());
            List<DataNode> dataNodes = function.calculate((Map) variables);
            if (dataNodes.size() != 1) {
                function.calculate((Map) variables);
                throw new IllegalArgumentException();
            }
            DataNode dataNode = Objects.requireNonNull(dataNodes.get(0));

            template.getValuesList().clear();
            SQLExprTableSource tableSource = template.getTableSource();
            tableSource.setExpr(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            template.addValueCause(valuesClause);


            List<Object> outParams = new ArrayList<>(params);
            StringBuilder sb = new StringBuilder();
            MycatPreparedStatementUtil.outputToParameters(template, sb, outParams);
            String sql = sb.toString();
            GroupKey key = GroupKey.of(sql, dataNode.getTargetName());
            Group group1 = group.computeIfAbsent(key, key1 -> new Group());
            group1.args.add(outParams);
        }
        return group;
    }

    @SneakyThrows
    private Map<GroupKey, Group> runMultiParams() {
        ShardingTableHandler logicTable = mycatInsertRel.getLogicTable();
        CustomRuleFunction function = logicTable.function();
        int finalAutoIncrementIndex = mycatInsertRel.getFinalAutoIncrementIndex();
        List<Integer> shardingKeys = mycatInsertRel.getShardingKeys();
        String[] columnNames = mycatInsertRel.getColumnNames();
        Supplier<Number> stringSupplier = logicTable.nextSequence();
        Map<GroupKey, Group> group = new HashMap<>();
        for (Object param : params) {
            MySqlInsertStatement mySqlInsertStatement = (MySqlInsertStatement) mycatInsertRel.getMySqlInsertStatement();
            List<Object> arg = (List<Object>) param;
            Number sequence = null;
            if (finalAutoIncrementIndex == -1 && logicTable.isAutoIncrement()) {
                arg.add(sequence = stringSupplier.get());
            }
            SQLInsertStatement.ValuesClause valuesClause = mySqlInsertStatement.getValues();
            List<SQLExpr> values = valuesClause.getValues();
            Map<String, List<RangeVariable>> variables = compute(shardingKeys, columnNames, values);
            List<DataNode> dataNodes = function.calculate((Map) variables);
            if (dataNodes.size() != 1) {
                throw new IllegalArgumentException();
            }
            DataNode dataNode = dataNodes.get(0);
            SQLExprTableSource tableSource = mySqlInsertStatement.getTableSource();
            tableSource.setExpr(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            String parameterizedString = mySqlInsertStatement.toParameterizedString();
            GroupKey key = GroupKey.of(parameterizedString, dataNode.getTargetName());
            Group group1 = group.computeIfAbsent(key, key1 -> new Group());
            group1.args.add(arg);
        }
        return group;
    }

    @SneakyThrows
    public void execute(Map<GroupKey, Group> group) {
        TransactionSession transactionSession = context.getTransactionSession();

        //建立targetName与连接的映射
        Map<String, MycatConnection> connections = new HashMap<>();
        Set<String> uniqueValues = new HashSet<>();
        for (GroupKey target : group.keySet()) {
            String k = transactionSession.resolveFinalTargetName(target.getTarget());
            if (uniqueValues.add(k)) {
                if (connections.put(target.getTarget(), transactionSession.getConnection(k)) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }
        }

        long lastInsertId = 0;
        long affected = 0;
        SqlRecord sqlRecord = context.currentSqlRecord();
        if (group.size() == 1) {
            Map.Entry<GroupKey, Group> keyGroupEntry = group.entrySet().iterator().next();
            String parameterizedSql = keyGroupEntry.getKey().getParameterizedSql();
            LinkedList<List<Object>> args = keyGroupEntry.getValue().getArgs();
            Connection connection = connections.values().iterator().next().unwrap(Connection.class);
            try (PreparedStatement preparedStatement = connection.
                    prepareStatement(parameterizedSql, Statement.RETURN_GENERATED_KEYS)) {
                List<Object> objects = args.get(0);
                setParams(preparedStatement, objects);

                long startTime = SqlRecord.now();

                affected = preparedStatement.executeUpdate();
                ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                if (generatedKeys != null) {
                    if (generatedKeys.next()) {
                        lastInsertId = generatedKeys.getLong(1);

                    }
                }
                String targetName = connections.keySet().iterator().next();
                sqlRecord.addSubRecord(parameterizedSql, startTime,targetName, affected);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("parameterizedSql:{} args:{} lastInsertId:{}", parameterizedSql, args, lastInsertId);
                }
            }
        } else {
            for (Map.Entry<GroupKey, Group> e : group.entrySet()) {
                GroupKey key = e.getKey();
                String targetName = (key.getTarget());
                String sql = key.getParameterizedSql();
                Group value = e.getValue();
                Connection connection = connections.get(targetName).unwrap(Connection.class);

                long startTime = SqlRecord.now();

                MycatPreparedStatementUtil.ExecuteBatchInsert res = MycatPreparedStatementUtil.batchInsert(sql, value, connection, targetName);
                lastInsertId = Math.max(lastInsertId, res.getLastInsertId());

                affected += res.getAffected();

                sqlRecord.addSubRecord(sql, startTime, targetName, affected);
            }
        }

        this.lastInsertId = lastInsertId;
        this.affectedRow = affected;
    }

    private Map<String, List<RangeVariable>> compute(List<Integer> shardingKeys,
                                                     String[] columnNames,
                                                     List<SQLExpr> values) {
        Map<String, List<RangeVariable>> variables = new HashMap<>(1);
        for (Integer shardingKey : shardingKeys) {
            SQLExpr sqlExpr = values.get(shardingKey);
            Object o = null;
            if (sqlExpr instanceof SQLVariantRefExpr) {
                throw new IllegalArgumentException();
            } else if (sqlExpr instanceof SQLNullExpr) {
                o = null;
            } else {
                o = SQLEvalVisitorUtils.eval(DbType.mysql, sqlExpr, params);
            }
            String columnName = columnNames[shardingKey];
            List<RangeVariable> rangeVariables = variables.computeIfAbsent(columnName, s -> new ArrayList<>(1));
            rangeVariables.add(new RangeVariable(columnName, RangeVariableType.EQUAL, o));
        }
        return variables;
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
        groupMap.forEach((k, v) -> {
            String target = k.getTarget();
            String parameterizedSql = k.getParameterizedSql();
            LinkedList<List<Object>> args = v.getArgs();
            writer.item("target:" + target + " parameterizedSql:" + parameterizedSql, args);
        });
        return explainWriter.ret();
    }

    public void onInsertSuccess() {
        ShardingTableHandler logicTable = mycatInsertRel.getLogicTable();
        if (!logicTable.canIndex()) {
            return;
        }
        GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
        if (gsiService == null) {
            return;
        }
        String[] columnNames = mycatInsertRel.getColumnNames();
        SimpleColumnInfo[] columns = new SimpleColumnInfo[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = logicTable.getColumnByName(columnNames[i]);
        }

        MycatDataContext mycatDataContext = MycatContext.CONTEXT.get();
        TransactionSession transactionSession = mycatDataContext.getTransactionSession();
        String txId = transactionSession.getTxId();
        for (Map.Entry<GroupKey, Group> entry : groupMap.entrySet()) {
            GroupKey key = entry.getKey();
            Group value = entry.getValue();
            LinkedList<List<Object>> args = value.getArgs();
            for (List<Object> arg : args) {
                gsiService.insert(txId, logicTable.getSchemaName(),
                        logicTable.getTableName(),columns, arg,key.getTarget());
            }
        }
    }
}