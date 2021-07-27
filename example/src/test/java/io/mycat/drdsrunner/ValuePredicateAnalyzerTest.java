package io.mycat.drdsrunner;

import io.mycat.DrdsSqlCompiler;
import io.mycat.MetadataManager;
import io.mycat.Partition;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.rewriter.ValueIndexCondition;
import io.mycat.calcite.rewriter.ValuePredicateAnalyzer;
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
public class ValuePredicateAnalyzerTest {
    private static final RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;


    @Test
    public void testTrue() {
        RexLiteral rexLiteral = rexBuilder.makeLiteral(true);
        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(
                Arrays.asList(KeyMeta.of("default", Arrays.asList("id"))),
                columnList
        );
        Map<QueryType, List<ValueIndexCondition>> indexCondition = valuePredicateAnalyzer2.translateMatch(rexLiteral);
        Assert.assertEquals("{}", indexCondition.toString());
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
        Assert.assertEquals(("[{targetName='c1', schemaName='db1_1', tableName='sharding_1', index=3, dbIndex=1, tableIndex=1}]").toString(), dataNodes.toString());
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

        Map<QueryType, List<ValueIndexCondition>> condition = valuePredicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals("{PK_RANGE_QUERY=[ValueIndexCondition(fieldNames=[id], indexName=default, indexColumnNames=[id], queryType=PK_RANGE_QUERY, rangeQueryLowerOp=GT, rangeQueryLowerKey=[0], rangeQueryUpperOp=LT, rangeQueryUpperKey=[1], pointQueryKey=null)]}", Objects.toString(condition));
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
        Object o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
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
        Map<QueryType, List<ValueIndexCondition>> indexCondition = valuePredicateAnalyzer2.translateMatch(RexUtil.composeConjunction(rexBuilder, Arrays.asList(leftRexNode, rightRexNode)));
        Assert.assertEquals("{PK_POINT_QUERY=[ValueIndexCondition(fieldNames=[id, id2], indexName=shardingDb, indexColumnNames=[id2], queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[?0]), ValueIndexCondition(fieldNames=[id, id2], indexName=shardingTable, indexColumnNames=[id], queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[0])]}", Objects.toString(indexCondition));

    }

    @Test
    public void testMultiOrEquals2() {
        DrdsTest.getDrds();
        List<RexNode> orList = new LinkedList<>();
        for (int i = 0; i < 2; i++) {
            RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                    MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                    0
            ), rexBuilder.makeLiteral(i, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
            orList.add(rexNode);
        }
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, orList);

        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(Arrays.asList(
                KeyMeta.of("seqSharding", "id")),
                columnList);
        Map<QueryType, List<ValueIndexCondition>> indexCondition = valuePredicateAnalyzer2.translateMatch(rexNode);
      Assert.assertEquals("{PK_POINT_QUERY=[ValueIndexCondition(fieldNames=[id], indexName=seqSharding, indexColumnNames=[id], queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[0, 1])]}", Objects.toString(indexCondition));
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "seqSharding");
        List o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
        o.sort(Comparator.comparing(c->c.toString()));
        Assert.assertEquals(2,o.size());
        Assert.assertEquals( "[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}]",Objects.toString(o));
        System.out.println();
    }
    @Test
    public void testMultiOrEquals3() {
        DrdsTest.getDrds();
        List<RexNode> orList = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                    MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                    0
            ), rexBuilder.makeLiteral(i, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
            orList.add(rexNode);
        }
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, orList);

        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(Arrays.asList(
                KeyMeta.of("seqSharding", "id")),
                columnList);
        Map<QueryType, List<ValueIndexCondition>> indexCondition = valuePredicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals("{PK_POINT_QUERY=[ValueIndexCondition(fieldNames=[id], indexName=seqSharding, indexColumnNames=[id], queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[0, 1, 2])]}", Objects.toString(indexCondition));
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "seqSharding");
        List o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
        o.sort(Comparator.comparing(c->c.toString()));
        Assert.assertEquals(3,o.size());
        Assert.assertEquals( "[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}, {targetName='c1', schemaName='db1_1', tableName='sharding_2', index=2, dbIndex=1, tableIndex=0}]",Objects.toString(o));
        System.out.println();
    }
    @Test
    public void testMultiOrEquals4() {
        DrdsTest.getDrds();
         int count = 4;
        List<RexNode> orList = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                    MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                    0
            ), rexBuilder.makeLiteral(i, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
            orList.add(rexNode);
        }
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, orList);

        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(Arrays.asList(
                KeyMeta.of("seqSharding", "id")),
                columnList);
        Map<QueryType, List<ValueIndexCondition>> indexCondition = valuePredicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals("{PK_POINT_QUERY=[ValueIndexCondition(fieldNames=[id], indexName=seqSharding, indexColumnNames=[id], queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[0, 1, 2, 3])]}", Objects.toString(indexCondition));
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "seqSharding");
        List o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
        o.sort(Comparator.comparing(c->c.toString()));
        Assert.assertEquals(count,o.size());
        Assert.assertEquals( "[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}, {targetName='c1', schemaName='db1_1', tableName='sharding_2', index=2, dbIndex=1, tableIndex=0}, {targetName='c1', schemaName='db1_1', tableName='sharding_3', index=3, dbIndex=1, tableIndex=1}]",Objects.toString(o));
        System.out.println();
    }
    @Test
    public void testMultiOrEquals5() {
        DrdsTest.getDrds();
        int count = 5;
        List<RexNode> orList = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(
                    MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER),
                    0
            ), rexBuilder.makeLiteral(i, MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.INTEGER), true));
            orList.add(rexNode);
        }
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, orList);

        List<String> columnList = Arrays.asList("id");
        ValuePredicateAnalyzer valuePredicateAnalyzer2 = new ValuePredicateAnalyzer(Arrays.asList(
                KeyMeta.of("seqSharding", "id")),
                columnList);
        Map<QueryType, List<ValueIndexCondition>> indexCondition = valuePredicateAnalyzer2.translateMatch(rexNode);
        Assert.assertEquals("{PK_POINT_QUERY=[ValueIndexCondition(fieldNames=[id], indexName=seqSharding, indexColumnNames=[id], queryType=PK_POINT_QUERY, rangeQueryLowerOp=null, rangeQueryLowerKey=null, rangeQueryUpperOp=null, rangeQueryUpperKey=null, pointQueryKey=[0, 1, 2, 3, 4])]}", Objects.toString(indexCondition));
        MetadataManager metadataManager = DrdsTest.getMetadataManager();
        ShardingTable table = (ShardingTable) metadataManager.getTable("db1", "seqSharding");
        List o = ValueIndexCondition.getObject(table.getShardingFuntion(), valuePredicateAnalyzer2.translateMatch(rexNode), Arrays.asList());
        o.sort(Comparator.comparing(c->c.toString()));
        Assert.assertEquals(4,o.size());
        Assert.assertEquals( "[{targetName='c0', schemaName='db1_0', tableName='sharding_0', index=0, dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', index=1, dbIndex=0, tableIndex=1}, {targetName='c1', schemaName='db1_1', tableName='sharding_2', index=2, dbIndex=1, tableIndex=0}, {targetName='c1', schemaName='db1_1', tableName='sharding_3', index=3, dbIndex=1, tableIndex=1}]",Objects.toString(o));
        System.out.println();
    }
}
