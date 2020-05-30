package io.mycat.mpp;


import com.alibaba.fastsql.sql.ast.SQLOrderingSpecification;
import io.mycat.mpp.element.Order;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.plan.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 构建SQL与关系表达式同构的上下文
 * 下推sql的最终模板是select xxxx from table group by xxxx having xxx order by xxx limit xx ,xxx
 * 其中table可能是join形式,join的条件是两物理表在同一分片
 * 0.MyRelBuilder.scan应该返回source对象,在addWhere的时候根据条件把它变成物理表(一个或者多个),最终Blackboard应该变成sql
 * 1.当遇上addOrderBy,addGroupBy,addLimit,如果此时的物理表是跨表的,如果存在最小连接使用的标记,则使用union合拼同一个分片的物理表.
 * 如果存在最大并行标记,则使用分库分表规则重写sql,以一个表一个连接发起查询的粒度进行重写(or表达式变成union,sum count)
 * 2.当出现关联子查询的时候(里层select 引用外层 select 字段),此后把节点最终转化为calcite能处理的类(sqlNode,RelNode,RexNode)
 * <p>
 * Blackboard,MyRelBuilder的设计目标是尽快确定sql在哪个模式下执行最高效,一般定义为存在关联子查询是复杂sql,
 * 理想情况下,发现存在关联子查询,应该把MyRelBuilder生产对象的目标设定为calcite,生产的对象应该接近calcite的sqlNode,RelNode,RexNode,并且把此前生产的对象一并转换
 * 如果没有发现关联子查询,则使用常见的分库分表规则处理sql即可,遇到要在mycat中执行的表达式运算,使用表达式树求值执行,这个引擎暂定名称mpp
 * mpp的类型系统可能是直到收到mysql响应的时候,收到各个数据源的字段报文时候,才可开始推导的,推导方式也是表达式树遍历求类型
 * (当然fastsql也能一定程度推导出select顶层字段的类型,但是如果出现推导的数据类型与实际收到的后端响应不一致,则要进行类型转换)
 */
public class Blackboard {
    private final MyRelBuilder rexBuilder;
    private final QueryPlan convertFrom;
    private SqlValue where;

    private List<SqlValue> selects;
    private List<String> aliasList;


    private Type type;
    private long offset = -1;
    private long count = -1;
    private List<Order> orders;
    private AggregationStep agg;


    public void addGroupBy(AggregationStep agg) {
        this.agg = agg;
    }

    enum Type {
        DUAL,
        ONE,
        MULTI
    }

    public Blackboard(MyRelBuilder rexBuilder, QueryPlan convertFrom) {
        this.rexBuilder = rexBuilder;
        this.convertFrom = Objects.requireNonNull(convertFrom);
    }

    public void addWhere(SqlValue convertExpr) {
        this.where = convertExpr;
    }

    public void addSelect(List<SqlValue> selects, List<String> aliasList) {
        this.selects = selects;
        this.aliasList = aliasList;
        if (convertFrom == null) {
            type = Type.DUAL;
        } else {
            type = Type.ONE;
        }

    }

    public void addOrderBy(List<Order> orders) {
        this.orders = orders;
    }


    public void addLimit(long offset, long to) {
        this.offset = offset;
        this.count = to;
    }

    public QueryPlan build() {
        QueryPlan out = convertFrom;
        if (where != null) {
            out = FilterPlan.create(out, this.where);
        }
        if (null != agg) {
            List<SqlValue> input = agg.getInput();
            out = ProjectPlan.create(out, input);

            List<AccessDataExpr> groupByItems = agg.getGroupByItems();
            ArrayList<io.mycat.mpp.runtime.Type > types = new ArrayList<>();
            int[] groupIds = new int[groupByItems.size()];
            int index = 0;
            for (AccessDataExpr groupByItem :groupByItems ) {
                groupIds[index++]= groupByItem.getIndex();
                types.add(groupByItem.getMySQLType());
            }

            List<AggSqlValue> aggregationExpr = agg.getAggregationExpr();
            int size = aggregationExpr.size();
            String[] funNames = new String[size];
            List<List<Integer>> args = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                AggSqlValue aggSqlValue = aggregationExpr.get(i);
                funNames[i] = aggSqlValue.getName();
                args.add(aggSqlValue.getArgs().stream().map(j -> ((AccessDataExpr) j).getIndex()).collect(Collectors.toList()));
            }

            out = AggregationPlan.create(out, funNames, args, RowType.of(agg.getOutput()), groupIds);
            if (agg.getHavingExpr() != null) {
                out = FilterPlan.create(out, agg.getHavingExpr());
            }
            out = ProjectPlan.create(out, agg.getOutput());

            System.out.println();
        }
        out = ProjectPlan.create(out, selects, this.aliasList);

//        if (groupList != null) {
//            int index = 0;
//            int groupListSize = this.groupList.size();
//            int[] fields = new int[groupListSize];
//            Column[] types = new Column[(this.groupList.size())];
//            for (SqlValue sqlValue : groupList) {
//                boolean isFinal = sqlValue instanceof AccessDataExpr;
//                if (!isFinal) {
//                    throw new UnsupportedOperationException("复杂查询");
//                }
//                AccessDataExpr abs = (AccessDataExpr) sqlValue;
//                fields[index] = abs.getIndex();
//                types[index] = (Column.of(abs.getColumnName(), abs.getType()));
//            }
//            rexBuilder.clear().push(out);
//            RowType type = out.getType();
//            out = AggregationPlan.create(out, new String[]{}, Collections.emptyList(),type,
//                    fields
//            );
//        }
        if (this.orders != null) {
            out = convertOrder(out);
        }
        if (count != -1 || offset != -1) {
            out = LimitPlan.create(out, offset, count);
        }
        return out;
    }

    @NotNull
    public QueryPlan convertOrder(QueryPlan out) {
        rexBuilder.clear().push(out);
        int[] fields = new int[this.orders.size()];
        boolean[] dir = new boolean[this.orders.size()];

        int index = 0;
        for (Order o : this.orders) {
            SqlValue sqlAbs = o.getSqlAbs();
            boolean isFinal = sqlAbs instanceof AccessDataExpr;
            if (!isFinal) {
                throw new UnsupportedOperationException("复杂查询");
            }
            AccessDataExpr abs = (AccessDataExpr) o.getSqlAbs();
            fields[index] = abs.getIndex();
            dir[index] = o.getType() == SQLOrderingSpecification.ASC;

            index++;
        }

        out = OrderPlan.create(out, fields, dir);
        return out;
    }
//        private void convertGroupBy(Blackboard blackboard, SQLSelectGroupByClause groupBy) {
//        List<SqlValue> groupList = new ArrayList<>();
//        for (SQLExpr item : groupBy.getItems()) {
//            groupList.add(convertExpr(item));
//        }
//        SQLExpr having = groupBy.getHaving();
//        if (having != null) {
//            blackboard.addGroupBy(groupList, convertExpr(having));
//        } else {
//            blackboard.addGroupBy(groupList, null);
//        }
}