package io.mycat.calcite.physical;

import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import groovy.sql.Sql;
import hu.akarnokd.rxjava3.operators.Observables;
import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSqlWithParams;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.rewriter.Distribution;
import io.reactivex.rxjava3.core.Observable;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.mycat.calcite.MycatImplementor.MYCAT_SQL_LOOKUP_IN;

@Getter
public class MycatSQLTableLookup extends SingleRel implements MycatRel {
    protected final RexNode condition;
    protected final JoinRelType joinType;
    protected final Type type;
    protected final List<CorrelationId> correlationIds;
    protected final MycatView right;

    public enum Type {
        NONE,
        BACK
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        JoinInfo joinInfo = JoinInfo.of(getInput(), right, condition);
        RelWriter relWriter = super.explainTerms(pw)
                .item("condition", condition)
                .item("joinType", joinType.lowerName)
                .item("type", type.toString())
                .item("correlationIds", correlationIds)
                .input("left", input)
                .input("right", right)
                .item("leftKeys",joinInfo.leftKeys);
        if (pw instanceof RelWriterImpl){
            pw.item("rightSQL",right.getSQLTemplate(false));
        }
        return relWriter;
    }

    public MycatSQLTableLookup changeTo(RelNode input, MycatView right) {
        return new MycatSQLTableLookup(getCluster(), input.getTraitSet(), input, right, joinType, condition, correlationIds, type);
    }

    public MycatSQLTableLookup changeTo(RelNode input) {
        return new MycatSQLTableLookup(getCluster(), input.getTraitSet(), input, right, joinType, condition, correlationIds, type);
    }

    public MycatSQLTableLookup(RelOptCluster cluster,
                               RelTraitSet traits,
                               RelNode input,
                               MycatView right,
                               JoinRelType joinType,
                               RexNode condition,
                               List<CorrelationId> correlationIds,
                               Type type) {
        super(cluster, traits.replace(MycatConvention.INSTANCE), input);
        this.condition = condition;
        this.joinType = joinType;
        this.correlationIds = correlationIds;
        this.type = type;
        this.right = right;
        switch (type) {
            case NONE:
                this.rowType = right.getRowType();
                break;
            case BACK:
                this.rowType = SqlValidatorUtil.deriveJoinRowType(input.getRowType(), right.getRowType(), joinType,
                        cluster.getTypeFactory(), null, ImmutableList.of());
                break;
        }
//
//        class ValueRowTypeFinder extends RelShuttleImpl {
//            public RelDataType argRowType;
//
//            @Override
//            public RelNode visit(RelNode other) {
//                if (other instanceof MycatTableLookupValues) {
//                    MycatTableLookupValues mycatTableLookupValues = (MycatTableLookupValues) other;
//                    this.argRowType = mycatTableLookupValues.getRowType();
//                }
//                return super.visit(other);
//            }
//        }
//        ValueRowTypeFinder valueRowTypeFinder = new ValueRowTypeFinder();
//        right.getRelNode().accept(valueRowTypeFinder);
//        this.argRowType = valueRowTypeFinder.argRowType;

    }

    public MycatSQLTableLookup(RelInput input) {
        this(input.getCluster(), input.getTraitSet(), input.getInput(), (MycatView) input.getInputs().get(1),
                input.getEnum("joinType", JoinRelType.class), input.getExpression("condition"),
                input.getIntegerList("correlationIds").stream().map(i -> new CorrelationId(i)).collect(Collectors.toList()),
                input.getEnum("type", Type.class));
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return implementStream((StreamMycatEnumerableRelImplementor) implementor, pref);
    }


    //hashJoin
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelNode left = getInput();
        double rowCount = mq.getRowCount(this);

        if (RelNodes.COMPARATOR.compare(left, right) > 0) {
            rowCount = RelMdUtil.addEpsilon(rowCount);
        }

        // Cheaper if the smaller number of rows is coming from the LHS.
        // Model this by adding L log L to the cost.
        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);
        if (Double.isInfinite(leftRowCount)) {
            rowCount = leftRowCount;
        } else {
            rowCount += Util.nLogN(leftRowCount);
        }
        if (Double.isInfinite(rightRowCount)) {
            rowCount = rightRowCount;
        } else {
            rowCount += rightRowCount;
        }
        RelOptCost relOptCost;
        relOptCost = planner.getCostFactory().makeCost(rowCount, 0, 0);
        return relOptCost;
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        RelNode left = getInput();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(), getRowType(), pref.preferArray());
        BlockBuilder builder = new BlockBuilder();
        Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        Expression leftExpression =
                toObservable(builder.append(
                        "left", leftResult.block));

        ParameterExpression root = implementor.getRootExpression();

        Method dispatch = Types.lookupMethod(MycatSQLTableLookup.class, "dispatchRightObservable", NewMycatDataContext.class, MycatSQLTableLookup.class, Observable.class);

        Result rightResult = implementor.result(
                physType,
                builder.append(Expressions.call(dispatch, root, implementor.stash(this, MycatSQLTableLookup.class), leftExpression)).toBlock());
        switch (type) {
            case NONE: {
                return rightResult;
            }
            case BACK: {
                return implementHashJoin(implementor, pref, leftResult, rightResult);
            }
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatSQLTableLookup(getCluster(),traitSet,inputs.get(0),right,joinType,condition,correlationIds,type);
    }

    public static Observable<Object[]> dispatchRightObservable(NewMycatDataContext context, MycatSQLTableLookup tableLookup, Observable<Object[]> leftInput) {
        MycatView rightView = (MycatView) tableLookup.getRight();
        CopyMycatRowMetaData rightRowMetaData = new CopyMycatRowMetaData(new CalciteRowMetaData(rightView.getRowType().getFieldList()));
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = (AsyncMycatDataContextImpl.SqlMycatDataContextImpl) context;

        Observable<Object[]> rightObservable = leftInput.buffer(300, 50).flatMap(argsList -> {
            RexShuttle rexShuttle = argSolver(argsList);
            RelNode mycatInnerRelNode = rightView.getRelNode().accept(new RelShuttleImpl() {
                @Override
                public RelNode visit(RelNode other) {
                    if (other instanceof Filter) {
                        Filter filter = (Filter) other;
                        return filter.accept(rexShuttle);
                    } else if (other instanceof MycatTableLookupValues) {
                        MycatTableLookupValues mycatTableLookupValues = (MycatTableLookupValues) other;
                        return mycatTableLookupValues.apply(argsList);
                    }
                    return super.visit(other);
                }
            });
            MycatView newRightView = new MycatView(rightView.getTraitSet(), mycatInnerRelNode, rightView.getDistribution(),
                    rightView.getCondition().map(c -> c.accept(rexShuttle)).orElse(null));
            DrdsSqlWithParams drdsSql = context.getDrdsSql();
            SqlNode sqlTemplate = newRightView.getSQLTemplate(DrdsSqlWithParams.isForUpdate(drdsSql.getParameterizedSql()));
            ImmutableMultimap<String, SqlString> apply1 = newRightView.apply(sqlTemplate, sqlMycatDataContext.getSqlMap(Collections.emptyMap(), newRightView, drdsSql, drdsSql.getHintDataNodeFilter()), drdsSql.getParams());
            return Observable.merge(sqlMycatDataContext.getObservables(apply1, rightRowMetaData));
        });
        return rightObservable;
    }

    @NotNull
    private static RexShuttle argSolver(List<Object[]> argsList) {
        return new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call.getOperator() == MYCAT_SQL_LOOKUP_IN) {
                    List<RexNode> operands = call.getOperands();
                    RexNode rexNode = operands.get(1);
                    RexCall rexCall = (RexCall) rexNode;
                    LinkedList<RexNode> accept = MycatTableLookupValues.apply(argsList, rexCall.getOperands());
                    accept.addFirst(operands.get(0));
                    return MycatCalciteSupport.RexBuilder.makeCall(call.getOperator(), accept);
                }
                return call;
            }
        };
    }

    class MycatValueArgFinder extends SqlShuttle {
        List<Object[]> params;

        public MycatValueArgFinder(List<Object[]> params) {
            this.params = params;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            List<SqlNode> operandList;
            SqlNode rightRow;
            SqlNode leftRow;
            LinkedList<SqlNode> sqlNodes = new LinkedList<>();
            if (call.getOperator() == MycatImplementor.MYCAT_SQL_VAULES || call.getOperator() == MycatImplementor.MYCAT_SQL_LOOKUP_IN) {
                operandList = call.getOperandList();
                rightRow = operandList.get(0);
                leftRow = operandList.get(1);
                sqlNodes.add(rightRow);

                for (Object[] param : params) {
                    sqlNodes.add(leftRow.accept(new SqlShuttle() {
                        @Override
                        public SqlNode visit(SqlDynamicParam sqlDynamicParam) {
                            Object object = param[sqlDynamicParam.getIndex()];
                            if (object == null) {
                                return SqlLiteral.createNull(SqlParserPos.ZERO);
                            } else {
                                return SqlLiteral.createCharString(object.toString(), SqlParserPos.ZERO);
                            }
                        }
                    }));
                }
                return new SqlBasicCall(call.getOperator(), sqlNodes.toArray(new SqlNode[]{}), SqlParserPos.ZERO);
            }
            return super.visit(call);
        }
    }

    ;

    private Result implementHashJoin(EnumerableRelImplementor implementor, Prefer pref, final Result leftResult, final Result rightResult) {
        RelNode left = input;
        JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        BlockBuilder builder = new BlockBuilder();
        Expression leftExpression =
                toEnumerate(builder.append(
                        "left", leftResult.block));
        Expression rightExpression =
                toEnumerate(builder.append(
                        "right", rightResult.block));
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(), getRowType(), pref.preferArray());
        final PhysType keyPhysType =
                leftResult.physType.project(
                        joinInfo.leftKeys, JavaRowFormat.LIST);
        Expression predicate = Expressions.constant(null);
        if (!joinInfo.nonEquiConditions.isEmpty()) {
            RexNode nonEquiCondition = RexUtil.composeConjunction(
                    getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
            if (nonEquiCondition != null) {
                predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
                        left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
            }
        }
        return implementor.result(
                physType,
                builder.append(
                        Expressions.call(
                                leftExpression,
                                BuiltInMethod.HASH_JOIN.method,
                                Expressions.list(
                                        rightExpression,
                                        leftResult.physType.generateAccessor(joinInfo.leftKeys),
                                        rightResult.physType.generateAccessor(joinInfo.rightKeys),
                                        EnumUtils.joinSelector(joinType,
                                                physType,
                                                ImmutableList.of(
                                                        leftResult.physType, rightResult.physType)))
                                        .append(
                                                Util.first(keyPhysType.comparer(),
                                                        Expressions.constant(null)))
                                        .append(
                                                Expressions.constant(joinType.generatesNullsOnLeft()))
                                        .append(
                                                Expressions.constant(
                                                        joinType.generatesNullsOnRight()))
                                        .append(predicate)))
                        .toBlock());
    }

    @Override
    public boolean isSupportStream() {
        return type == Type.NONE;
    }
}
