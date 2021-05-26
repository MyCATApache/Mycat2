package io.mycat.calcite.physical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.mycat.DataNode;
import io.mycat.RangeVariable;
import io.mycat.RangeVariableType;
import io.mycat.SimpleColumnInfo;
import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.router.CustomRuleFunction;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.eclipse.collections.api.multimap.ImmutableMultimap;
import org.eclipse.collections.api.tuple.Pair;

import java.lang.reflect.Method;
import java.util.*;

public class MycatTableLookup extends SingleRel implements MycatRel {
    private final RelNode right;

    protected MycatTableLookup(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelNode right) {
        super(cluster, traits.replace(MycatConvention.INSTANCE), input);
        if (right instanceof MycatView){
            right = ((MycatView) right).getRelNode();
        }
        this.right = right;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }

    @Override
    public void explain(RelWriter pw) {
        super.explain(pw);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("right",right
        );
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
//        Project project = (Project) this.right;
//        Filter filter = project.getInput() instanceof Filter ? (Filter) project.getInput() : null;
//        Join join = filter != null ? (Join) filter.getInput() : (Join) project.getInput();
//        Values left = (Values) join.getLeft();
//        TableScan right = (TableScan) join.getRight();
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
        Expression leftExpression =
                toEnumerate(builder.append(
                        "left", leftResult.block));

        ParameterExpression root = implementor.getRootExpression();
        Method asList = Types.lookupMethod(RxBuiltInMethodImpl.class, "asList", Object.class);
        Method dispatch = Types.lookupMethod(MycatTableLookup.class, "dispatch", NewMycatDataContext.class, MycatTableLookup.class, List.class);
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        builder.add(Expressions.call(dispatch, root, implementor.stash(this, MycatTableLookup.class), Expressions.call(asList, leftExpression)));
        return implementor.result(physType, builder.toBlock());
    }

    private static Map<String, List<SqlString>> route(ShardingTable shardingTable,MycatTableLookup relNode, List<Object[]> keys) {
        SqlNode sqlNode = MycatCalciteSupport.INSTANCE.convertToSqlTemplate(relNode.right, MycatSqlDialect.DEFAULT, false);
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sqlNode.toString());
        ImmutableMultimap<DataNode, List<Object[]>> nodeListMap = routeKeys(shardingTable,relNode, keys);
        Map<String, List<SqlString>> sqls = new HashMap<>();
        for (Pair<DataNode, List<Object[]>> dataNodeListPair : nodeListMap.keyValuePairsView()) {
            DataNode dataNode = dataNodeListPair.getOne();
            List<Object[]> objects = dataNodeListPair.getTwo();
            SQLStatement curStatement = sqlStatement.clone();
            curStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLValuesTableSource x) {
                    List<SQLName> columns = x.getColumns();
                    List<SQLListExpr> values = x.getValues();
                    values.clear();
                    for (Object[] key : objects) {
                        SQLListExpr sqlListExpr = new SQLListExpr();
                        for (Object o : key) {
                            sqlListExpr.addItem(SQLExprUtils.fromJavaObject(o));
                        }
                        values.add(sqlListExpr);
                    }
                    return false;
                }

                @Override
                public boolean visit(SQLExprTableSource x) {
                    x.setSimpleName(dataNode.getTable());
                    x.setSchema(dataNode.getSchema());
                    return false;
                }
            });
            List<SqlString> strings = sqls.computeIfAbsent(dataNode.getTargetName(), s -> new ArrayList<>());
            strings.add(new SqlString(MycatSqlDialect.DEFAULT, curStatement.toString(), ImmutableList.of()));
        }
        return sqls;
    }

    private static ImmutableMultimap<DataNode, List<Object[]>> routeKeys(ShardingTable shardingTable,MycatTableLookup relNode, List<Object[]> keys) {
        CustomRuleFunction shardingFuntion = shardingTable.getShardingFuntion();
        List<String> fieldNames = relNode.getInput().getRowType().getFieldNames();
        SimpleColumnInfo[] columns = new SimpleColumnInfo[fieldNames.size()];
        int index = 0;
        for (String fieldName : fieldNames) {
            if(shardingFuntion.isShardingKey(fieldName)){
                columns[index] = shardingTable.getColumnByName(fieldName);
            }
            index++;
        }
        ImmutableListMultimap.Builder<DataNode, List<Object[]>> resBuilder = ImmutableListMultimap.builder();

        for (Object[] key : keys) {
            ImmutableMap.Builder<String, Collection<RangeVariable>> builder = ImmutableMap.builder();
            for (int i = 0; i < columns.length; i++) {
                SimpleColumnInfo column = columns[i];
                if (column!=null){
                    String columnName = column.getColumnName();
                    builder.put(columnName,  Collections.singleton(new RangeVariable(columnName,RangeVariableType.EQUAL,column.normalizeValue(key[i]))));
                }
            }
            List<DataNode> dataNodes = shardingTable.getShardingFuntion().calculate(builder.build());
            for (DataNode dataNode : dataNodes) {
                resBuilder.put(dataNode,keys);
            }
        }
       return (ImmutableMultimap<DataNode, List<Object[]>>) resBuilder.build();
    }

    public static Enumerable<Object[]> dispatch(NewMycatDataContext context, MycatTableLookup mycatTableLookup, List<Object[]> keys) {
        List<RelOptTable> allTables = RelOptUtil.findAllTables(mycatTableLookup.right);
        RelOptTable relOptTable = allTables.get(0);
        MycatLogicTable mycatLogicTable = relOptTable.unwrap(MycatLogicTable.class);
        ShardingTable shardingTable = (ShardingTable) mycatLogicTable.logicTable();
        return context.getEnumerable(ExecutorSupport.physicalSqlMerge(route(shardingTable,mycatTableLookup, keys), false));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatTableLookup(getCluster(), traitSet, inputs.get(0), inputs.get(1));
    }
}
