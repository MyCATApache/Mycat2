package io.mycat.drdsrunner;

import io.mycat.*;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.rewriter.ValueIndexCondition;
import io.mycat.calcite.rewriter.ValuePredicateAnalyzer;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.querycondition.ComparisonOperator;
import io.mycat.querycondition.KeyMeta;
import io.mycat.querycondition.QueryType;
import io.mycat.router.CustomRuleFunction;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ValuePredicateAnalyzerTest {
    private static final RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;


    @Test
    public void testTrue() {
        RexLiteral rexLiteral = rexBuilder.makeLiteral(true);
        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", "id")),
                columnList
        );
        ValueIndexCondition indexCondition = valuePredicateAnalyzer2.translateMatch(rexLiteral);
        Assert.assertEquals(QueryType.PK_FULL_SCAN, indexCondition.getQueryType());
    }

    @Test
    public void testEqual() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(1, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Collections.singletonList(KeyMeta.of("default", "id")), columnList);
        List<Partition> dataNodes = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList(1));
        Assert.assertEquals(("[{targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}]").toString(), dataNodes.toString());
    }

    @Test
    public void testEqualParam() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(1, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", "id")),
                columnList
        );
        List<Partition> dataNodes = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList(0));
        Assert.assertEquals(1, dataNodes.size());
    }

    @Test
    public void testRange() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(0, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));

        RexNode rightRexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(1, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));

        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, leftRexNode, rightRexNode);
        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", "id")),
                columnList
        );
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");

        ValueIndexCondition condition = valuePredicateAnalyzer2.translateMatch(rexNode);
        List<Object> params = Arrays.asList(0, 1);

        List<Object> pointQueryKey = ValueIndexCondition.resolveParam(null, condition.getPointQueryKey());
        ComparisonOperator rangeQueryLowerOp = condition.getRangeQueryLowerOp();
        List<Object> rangeQueryLowerKey = (List) (condition.getRangeQueryLowerKey());
        ComparisonOperator rangeQueryUpperOp = condition.getRangeQueryUpperOp();
        List<Object> rangeQueryUpperKey = (List) condition.getRangeQueryUpperKey();

        Map<String, Collection<RangeVariable>> map = new HashMap<>();
        CustomRuleFunction customRuleFunction = table.getShardingFuntion();
//        Assert.assertEquals(PK_RANGE_QUERY, );


        if (rangeQueryUpperOp == ComparisonOperator.LT) {
            rangeQueryUpperOp = ComparisonOperator.LTE;
        }
        if (rangeQueryLowerOp == ComparisonOperator.GT) {
            rangeQueryLowerOp = ComparisonOperator.GTE;
        }
        Assert.assertEquals(ComparisonOperator.LTE, rangeQueryUpperOp);
        Assert.assertEquals(ComparisonOperator.GTE, rangeQueryLowerOp);
        if (rangeQueryUpperOp == ComparisonOperator.LTE && rangeQueryLowerOp == ComparisonOperator.GTE) {
//            ArrayList<Object> leftValues = new ArrayList<>();
//            for (Object o1 : rangeQueryLowerKey) {
//                if (o1 instanceof RexNode) {
//                    o1 = io.mycat.calcite.rewriter.MycatRexUtil.resolveParam((RexNode) o1, params);
//                }
//                leftValues.add(o1);
//            }
//            ArrayList<Object> rightValues = new ArrayList<>();
//            for (Object o1 : rangeQueryUpperKey) {
//                if (o1 instanceof RexNode) {
//                    o1 = io.mycat.calcite.rewriter.MycatRexUtil.resolveParam((RexNode) o1, params);
//                }
//                rightValues.add(o1);
//            }
            Collections.sort((List) rangeQueryLowerKey);
            Collections.sort((List) rangeQueryUpperKey);

            Object smallOne = rangeQueryLowerKey.get(0);
            Object bigOne = rangeQueryUpperKey.get(0);

            RangeVariable rangeVariable = new RangeVariable(condition.getIndexColumnNames(), RangeVariableType.RANGE, smallOne, bigOne);
            Assert.assertEquals(new RangeVariable("id", RangeVariableType.RANGE,
                    (0)
                    , (1)).toString(), rangeVariable.toString());
        }

        Object o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), params);
        Assert.assertEquals("[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}, {targetName='c1', schemaName='db1_1', tableName='sharding_0', index=2, dbIndex=1, tableIndex=0}, {targetName='c1', schemaName='db1_1', tableName='sharding_1', index=3, dbIndex=1, tableIndex=1}]", Objects.toString(o));
    }

    @Test
    public void testOr() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(0, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));

        RexNode rightRexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, leftRexNode, rightRexNode);
        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", "id")),
                columnList
        );
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        Object o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
        Assert.assertEquals("[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}, {targetName='c1', schemaName='db1_1', tableName='sharding_0', index=2, dbIndex=1, tableIndex=0}, {targetName='c1', schemaName='db1_1', tableName='sharding_1', index=3, dbIndex=1, tableIndex=1}]", Objects.toString(o));
    }

    //
    @Test
    public void testNot() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(0, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, leftRexNode);
        List<String> columnList = Arrays.asList("id");
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", "id")),
                columnList
        );
        Object o =ValueIndexCondition. getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
        Assert.assertEquals("[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}, {targetName='c1', schemaName='db1_1', tableName='sharding_0', index=2, dbIndex=1, tableIndex=0}, {targetName='c1', schemaName='db1_1', tableName='sharding_1', index=3, dbIndex=1, tableIndex=1}]", Objects.toString(o));

    }

    @Test
    public void testDoubleColumns() {
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeLiteral(0, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
        RexNode rightRexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                1
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        List<String> columnList = Arrays.asList("id", "id2");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(Arrays.asList(
                KeyMeta.of("shardingTable", "id"),
                KeyMeta.of("shardingDb", "id2")),
                columnList);
        ValueIndexCondition indexCondition = valuePredicateAnalyzer2.translateMatch(RexUtil.composeConjunction(rexBuilder, Arrays.asList(leftRexNode, rightRexNode)));
        Assert.assertEquals("ValueIndexCondition(fieldNames=[id, id2], indexName=shardingTable, indexColumnNames=id, queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[0])", Objects.toString(indexCondition));

    }


}
