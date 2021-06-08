package io.mycat.calcite.executor;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.MycatSQLEvalVisitorUtils;
import io.mycat.*;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatRouteInsertCore;
import io.mycat.gsi.GSIService;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.FastSqlUtils;
import io.mycat.util.Pair;
import io.mycat.util.SQL;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

@Getter
public class MycatInsertExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatInsertExecutor.class);

    private MycatDataContext context;
    private final MycatInsertRel mycatInsertRel;
    /**
     * 最终发给后端的sql, 包含全部字段的数据 （比如自增ID）
     */
    private final Map<SQL, Group> groupMap;
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

    public MycatInsertExecutor(MycatDataContext context, MycatInsertRel mycatInsertRel, List<Object> params) {
        this.context = context;
        this.mycatInsertRel = mycatInsertRel;
        this.params = params;

        this.multi = !params.isEmpty() && (params.get(0) instanceof List);
        if (multi) {
            this.groupMap = runMultiParams();
        } else {
            this.groupMap = runNormalParams();
        }
    }

    public boolean isProxy() {
        return groupMap.keySet().size() == 1;
    }

    public Pair<String, String> getSingleSql() {
        Map.Entry<SQL, Group> entry = groupMap.entrySet().iterator().next();
        SQL key = entry.getKey();
        String parameterizedSql = key.getParameterizedSql();
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
        return Pair.of(context.resolveDatasourceTargetName(key.getTarget(), true), sqlStatement.toString());
    }

    public static MycatInsertExecutor create(MycatDataContext context, MycatInsertRel mycatInsertRel, List<Object> params) {
        return new MycatInsertExecutor(context, mycatInsertRel, params);
    }

//    @Override
//    public void open() {
//        try {
//            execute(groupMap);
//            onInsertSuccess();
//        } finally {
//            done = true;
//        }
//    }

    @SneakyThrows
    private Map<SQL, Group> runNormalParams() {
        MycatRouteInsertCore mycatRouteInsertCore = mycatInsertRel.getMycatRouteInsertCore();
        MySqlInsertStatement mySqlInsertStatement = mycatRouteInsertCore.getMySqlInsertStatement();
        ShardingTableHandler logicTable = mycatRouteInsertCore.logicTable();
        CustomRuleFunction function = logicTable.function();
        int finalAutoIncrementIndex = mycatRouteInsertCore.getFinalAutoIncrementIndex();
        List<Integer> shardingKeys = mycatRouteInsertCore.getShardingKeys();
        String[] columnNames = mycatRouteInsertCore.getColumnNames();
        Supplier<Number> stringSupplier = logicTable.nextSequence();

        Map<SQL, Group> group = new HashMap<>();
        int count = 0;
        for (SQLInsertStatement.ValuesClause valuesClause : mySqlInsertStatement.getValuesList()) {
            MySqlInsertStatement cloneStatement = FastSqlUtils.clone(mySqlInsertStatement);
            List<SQLInsertStatement.ValuesClause> valuesList = cloneStatement.getValuesList();
            valuesList.clear();

            boolean fillSequence = finalAutoIncrementIndex == -1 && logicTable.isAutoIncrement();
            Number sequence;
            if (fillSequence) {
                sequence = stringSupplier.get();
                valuesClause.addValue(SQLExprUtils.fromJavaObject(sequence));
            }

            Map<String, List<RangeVariable>> variables = compute(shardingKeys, columnNames, valuesClause.getValues(), params);
            Partition partition = function.calculateOne((Map) variables);

            SQLExprTableSource tableSource = cloneStatement.getTableSource();
            tableSource.setExpr(partition.getTable());
            tableSource.setSchema(partition.getSchema());
            cloneStatement.addValueCause(valuesClause);

            int size = valuesClause.getValues().size();
            int startIndex = count * size;
            List<Object> outParams = new ArrayList<>();

            cloneStatement.accept(new MySqlASTVisitorAdapter(){
                @Override
                public boolean visit(SQLVariantRefExpr x) {
                    outParams.add(params.get(x.getIndex()));
                    return false;
                }
            });

            String parameterizedString = cloneStatement.toString();
            SQL key = SQL.of(parameterizedString, partition, cloneStatement, outParams);
            Group group1 = group.computeIfAbsent(key, key1 -> new Group());
            group1.args.add(outParams);

            count++;
        }
        return group;
    }

    @SneakyThrows
    private Map<SQL, Group> runMultiParams() {
        MycatRouteInsertCore mycatRouteInsertCore = mycatInsertRel.getMycatRouteInsertCore();
        ShardingTableHandler logicTable = mycatRouteInsertCore.logicTable();
        CustomRuleFunction function = logicTable.function();
        int finalAutoIncrementIndex = mycatRouteInsertCore.getFinalAutoIncrementIndex();
        List<Integer> shardingKeys = mycatRouteInsertCore.getShardingKeys();
        String[] columnNames = mycatRouteInsertCore.getColumnNames();
        Supplier<Number> stringSupplier = logicTable.nextSequence();
        Map<SQL, Group> group = new HashMap<>();
        for (Object param : params) {
            MySqlInsertStatement mySqlInsertStatement = (MySqlInsertStatement) mycatRouteInsertCore.getMySqlInsertStatement();
            List<Object> arg = (List<Object>) param;
            Number sequence = null;
            SQLInsertStatement.ValuesClause valuesClause = mySqlInsertStatement.getValues();

            if (finalAutoIncrementIndex == -1 && logicTable.isAutoIncrement()) {
                arg.add(sequence = stringSupplier.get());
                SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr();
                sqlVariantRefExpr.setIndex(valuesClause.getValues().size());
                sqlVariantRefExpr.setName("?");
                valuesClause.addValue(sqlVariantRefExpr);
            }

            Map<String, List<RangeVariable>> variables = compute(shardingKeys, columnNames, valuesClause.getValues(), (List) param);
            Partition partition = function.calculateOne((Map) variables);
            SQLExprTableSource tableSource = mySqlInsertStatement.getTableSource();
            tableSource.setExpr(partition.getTable());
            tableSource.setSchema(partition.getSchema());

            StringBuilder sb = new StringBuilder();
            List<Object> out = new ArrayList<>();
            MycatPreparedStatementUtil.outputToParameterized(mySqlInsertStatement, sb, out);
            String parameterizedString = sb.toString();
            SQL key = SQL.of(parameterizedString, partition, mySqlInsertStatement, arg);
            Group group1 = group.computeIfAbsent(key, key1 -> new Group());
            group1.args.add(arg);
        }
        return group;
    }

    private static Map<String, List<RangeVariable>> compute(List<Integer> shardingKeys,
                                                            String[] columnNames,
                                                            List<SQLExpr> values,
                                                            List<Object> params) {
        Map<String, List<RangeVariable>> variables = new HashMap<>(1);
        for (Integer shardingKey : shardingKeys) {
            SQLExpr sqlExpr = values.get(shardingKey);
            Object o = null;
            if (sqlExpr instanceof SQLVariantRefExpr) {
                int index = ((SQLVariantRefExpr) sqlExpr).getIndex();
                o = params.get(index);
            } else if (sqlExpr instanceof SQLNullExpr) {
                o = null;
            } else {
                try {
                    o = MycatSQLEvalVisitorUtils.eval(DbType.mysql, sqlExpr, params);
                } catch (Throwable throwable) {
                    boolean success = false;
                    if (sqlExpr instanceof SQLMethodInvokeExpr) {
                        if (!((SQLMethodInvokeExpr) sqlExpr).getArguments().isEmpty()) {
                            SQLExpr sqlExpr1 = ((SQLMethodInvokeExpr) sqlExpr).getArguments().get(0);
                            if (sqlExpr1 instanceof SQLVariantRefExpr) {
                                int index = ((SQLVariantRefExpr) sqlExpr1).getIndex();
                                o = params.get(index);
                                success =true;
                            }
                        }
                    }
                    if (!success){
                        throw  throwable;
                    }
                }
            }
            String columnName = columnNames[shardingKey];
            List<RangeVariable> rangeVariables = variables.computeIfAbsent(columnName, s -> new ArrayList<>(1));
            rangeVariables.add(new RangeVariable(columnName, RangeVariableType.EQUAL, o));
        }
        return variables;
    }



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
        MycatRouteInsertCore mycatRouteInsertCore = mycatInsertRel.getMycatRouteInsertCore();
        ShardingTableHandler logicTable = mycatRouteInsertCore.logicTable();
        if (!logicTable.canIndex()) {
            return;
        }
        if (!MetaClusterCurrent.exist(GSIService.class)) {
            return;
        }
        GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
        MycatDataContext mycatDataContext = this.context;
        TransactionSession transactionSession = mycatDataContext.getTransactionSession();
        String txId = transactionSession.getXid();

        String[] columnNames = mycatRouteInsertCore.getColumnNames();
        SimpleColumnInfo[] columns = new SimpleColumnInfo[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = logicTable.getColumnByName(columnNames[i]);
        }

        for (Map.Entry<SQL, Group> entry : groupMap.entrySet()) {
            SQL key = entry.getKey();
            Group value = entry.getValue();
            LinkedList<List<Object>> args = value.getArgs();
            for (List<Object> arg : args) {
                gsiService.insert(txId, logicTable.getSchemaName(),
                        logicTable.getTableName(), columns, arg, key.getTarget());
            }
        }
    }
}