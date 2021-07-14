package io.mycat.drdsrunner;

import io.mycat.*;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.calcite.rewriter.PredicateAnalyzer;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.querycondition.KeyMeta;
import io.mycat.querycondition.QueryType;
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
public class PredicateAnalyzerTest {
    private static final RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;


    @Test
    public void testTrue() {
        RexLiteral rexLiteral = rexBuilder.makeLiteral(true);
        List<String> columnList = Arrays.asList("id");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", Arrays.asList("id"))),
                columnList
        );
        Map<QueryType, List<IndexCondition>> map = predicateAnalyzer2.translateMatch(rexLiteral);
        Assert.assertEquals(0, map.size());
    }

    @Test
    public void testEqual() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        List<String> columnList = Arrays.asList("id");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(
                Collections.singletonList(KeyMeta.of("default", Arrays.asList("id"))), columnList);
        Map<QueryType, List<IndexCondition>> queryTypeListMap = predicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals(1, queryTypeListMap.size());
        Map.Entry<QueryType, List<IndexCondition>> entry = queryTypeListMap.entrySet().iterator().next();
        Assert.assertEquals(QueryType.PK_POINT_QUERY, entry.getKey());
        Assert.assertEquals(1, entry.getValue().size());
        Assert.assertEquals("[id]", entry.getValue().get(0).getIndexColumnNames().toString());
    }

    @Test
    public void testEqualParam() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        List<String> columnList = Arrays.asList("id");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", Arrays.asList("id"))),
                columnList
        );
        Map<QueryType, List<IndexCondition>> queryTypeListMap = predicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals(1, queryTypeListMap.size());
        Map.Entry<QueryType, List<IndexCondition>> entry = queryTypeListMap.entrySet().iterator().next();
        Assert.assertEquals(QueryType.PK_POINT_QUERY, entry.getKey());
        Assert.assertEquals(1, entry.getValue().size());
        Assert.assertEquals("[id]", entry.getValue().get(0).getIndexColumnNames().toString());
        System.out.println();
    }

    @Test
    public void testRange() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));

        RexNode rightRexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, leftRexNode, rightRexNode);
        List<String> columnList = Arrays.asList("id");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", Arrays.asList("id"))),
                columnList
        );
        Map<QueryType, List<IndexCondition>> queryTypeListMap = predicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals(1, queryTypeListMap.size());
        Map.Entry<QueryType, List<IndexCondition>> entry = queryTypeListMap.entrySet().iterator().next();
        Assert.assertEquals(QueryType.PK_RANGE_QUERY, entry.getKey());
        Assert.assertEquals(1, entry.getValue().size());
        Assert.assertEquals("[id]", entry.getValue().get(0).getIndexColumnNames().toString());
        System.out.println();
    }

    //
    @Test
    public void testOr() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));

        RexNode rightRexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, leftRexNode, rightRexNode);
        List<String> columnList = Arrays.asList("id");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", Arrays.asList("id"))),
                columnList
        );
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");

        Map<QueryType, List<IndexCondition>> queryTypeListMap = predicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals(0,queryTypeListMap.size());
        System.out.println();
    }

    //
    @Test
    public void testNot() {
        DrdsSqlCompiler drds = DrdsTest.getDrds();
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, leftRexNode);
        List<String> columnList = Arrays.asList("id");
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "sharding");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", Arrays.asList("id"))),
                columnList
        );

    }

    @Test
    public void testDoubleColumns() {
        RexNode leftRexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                0
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        RexNode rightRexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                1
        ), rexBuilder.makeDynamicParam(MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        List<String> columnList = Arrays.asList("id", "id2");
        PredicateAnalyzer predicateAnalyzer2 = new PredicateAnalyzer(Arrays.asList(
                KeyMeta.of("shardingTable", Arrays.asList("id")),
                KeyMeta.of("shardingDb",  Arrays.asList("id2"))),
                columnList);
        Map<QueryType, List<IndexCondition>> queryTypeListMap = new TreeMap<>(
                predicateAnalyzer2.translateMatch(RexUtil.composeConjunction(rexBuilder, Arrays.asList(leftRexNode, rightRexNode)))
        );

        Assert.assertEquals(1, queryTypeListMap.size());
        Map.Entry<QueryType, List<IndexCondition>> entry = queryTypeListMap.entrySet().iterator().next();
        Assert.assertEquals(QueryType.PK_POINT_QUERY, entry.getKey());
        Assert.assertEquals(2, entry.getValue().size());
        String indexColumnName0 = entry.getValue().get(0).getIndexColumnNames().get(0);
        Assert.assertEquals("id2", indexColumnName0);
        String indexColumnName1 = entry.getValue().get(1).getIndexColumnNames().get(0);
        Assert.assertEquals("id", indexColumnName1);
        System.out.println();

    }


}
