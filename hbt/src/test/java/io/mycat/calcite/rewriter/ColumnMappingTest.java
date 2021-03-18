package io.mycat.calcite.rewriter;

import io.mycat.DrdsRunner;
import io.mycat.calcite.MycatCalciteSupport;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.calcite.rel.core.JoinRelType.LEFT;

public class ColumnMappingTest {
    public static RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;

    @Test
    public void testSingleTable() {
        RelBuilder relBuilder = createRelBuilder();
        RelNode relNode = createTable_user(relBuilder);

        ColumnMapping2 columnMapping = new ColumnMapping2();
        relNode.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(relNode, left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(1);
        Assert.assertEquals(1, right.getIndex());
        Assert.assertEquals(relNode, right.getTableScan());
    }

    private RelNode createTable_user(RelBuilder relBuilder) {
        RelDataTypeFactory.FieldInfoBuilder builder = MycatCalciteSupport.TypeFactory.builder();
        builder.add("id", SqlTypeName.INTEGER);
        builder.add("username", SqlTypeName.INTEGER);
        RelDataType relDataType = builder.build();
        return relBuilder.transientScan("user", relDataType).build();
    }

    private RelNode createTable_user2(RelBuilder relBuilder) {
        RelDataTypeFactory.FieldInfoBuilder builder = MycatCalciteSupport.TypeFactory.builder();
        builder.add("id", SqlTypeName.INTEGER);
        builder.add("username", SqlTypeName.INTEGER);
        RelDataType relDataType = builder.build();
        return relBuilder.transientScan("user2", relDataType).build();
    }

    private RelNode createTable_test(RelBuilder relBuilder) {
        RelDataTypeFactory.FieldInfoBuilder builder = MycatCalciteSupport.TypeFactory.builder();
        builder.add("id", SqlTypeName.INTEGER);
        RelDataType relDataType = builder.build();
        return relBuilder.transientScan("test", relDataType).build();
    }

    @Test
    public void testFilterTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode relNode = createTable_user(relBuilder);
        relNode = relBuilder.push(relNode).filter(rexBuilder.makeLiteral(true)).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        relNode.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(relNode, left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(1);
        Assert.assertEquals(1, right.getIndex());
        Assert.assertEquals(relNode, right.getTableScan());
    }

    @Test
    public void testSortTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode relNode = createTable_user(relBuilder);
        relNode = relBuilder.push(relNode).sort(0).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        relNode.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(relNode.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(1);
        Assert.assertEquals(1, right.getIndex());
        Assert.assertEquals(relNode.getInput(0), right.getTableScan());
    }

    @Test
    public void testProjectTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode relNode = createTable_user(relBuilder);
        relNode = RelOptUtil.createProject(relNode, Arrays.asList(1, 0));
        ColumnMapping2 columnMapping = new ColumnMapping2();
        relNode.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(1, left.getIndex());
        Assert.assertEquals(relNode.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(1);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(relNode.getInput(0), right.getTableScan());
    }

    @Test
    public void testProjectExprTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode relNode = createTable_user(relBuilder);
        relBuilder.push(relNode);
        RexNode plus = rexBuilder.makeCall
                (SqlStdOperatorTable.PLUS, relBuilder.field(0), rexBuilder.makeExactLiteral(BigDecimal.ONE));
        relNode = relBuilder.project(plus,
                relBuilder.field(0),
                relBuilder.field(1)).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        relNode.accept(columnMapping);

        ColumnInfo first = columnMapping.getBottomColumnInfo(0);


        Assert.assertNull(first);

        ColumnInfo left = columnMapping.getBottomColumnInfo(1);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(relNode.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(1, right.getIndex());
        Assert.assertEquals(relNode.getInput(0), right.getTableScan());
    }

    @Test
    public void testInnerJoinTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode joinTable = relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true)).build();
        Assert.assertEquals(4, joinTable.getRowType().getFieldCount());

        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(joinTable.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(joinTable.getInput(1), right.getTableScan());
    }

    @Test
    public void testLeftJoinTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode joinTable = relBuilder.join(LEFT, rexBuilder.makeLiteral(true)).build();
        Assert.assertEquals(4, joinTable.getRowType().getFieldCount());

        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(joinTable.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(joinTable.getInput(1), right.getTableScan());
    }

    @Test
    public void testRightJoinTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode joinTable = relBuilder.join(JoinRelType.RIGHT, rexBuilder.makeLiteral(true)).build();
        Assert.assertEquals(4, joinTable.getRowType().getFieldCount());

        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(joinTable.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(joinTable.getInput(1), right.getTableScan());
    }

    @Test
    public void testFullJoinTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode joinTable = relBuilder.join(JoinRelType.FULL, rexBuilder.makeLiteral(true)).build();
        Assert.assertEquals(4, joinTable.getRowType().getFieldCount());

        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(joinTable.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(joinTable.getInput(1), right.getTableScan());
    }

    @Test
    public void testSemiJoinTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode joinTable = relBuilder.semiJoin(rexBuilder.makeLiteral(true)).build();
        Assert.assertEquals(2, joinTable.getRowType().getFieldCount());
        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(joinTable.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(joinTable.getInput(1), right.getTableScan());
    }

    @Test
    public void testAntiJoinTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode joinTable = relBuilder.antiJoin(rexBuilder.makeLiteral(true)).build();
        Assert.assertEquals(2, joinTable.getRowType().getFieldCount());
        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(joinTable.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(joinTable.getInput(1), right.getTableScan());
    }

    /**
     * case LEFT:
     * case INNER:
     * return SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
     * right.getRowType(), joinType,
     * getCluster().getTypeFactory(), null,
     * ImmutableList.of());
     * case ANTI:
     * case SEMI:
     */
    @Test
    public void testLeftCorrelateTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);

        final Holder<RexCorrelVariable> v = Holder.of(null);
        RelNode joinTable = relBuilder
                .push(leftTable)
                .variable(v)
                .push(rightTable)
                .filter(
                        relBuilder.equals(relBuilder.field(0), relBuilder.field(v.get(), "username")))
                .correlate(
                        JoinRelType.LEFT, v.get().id, relBuilder.field(2, 0, "username"))
                .build();
        System.out.println(joinTable);


        Assert.assertEquals(4, joinTable.getRowType().getFieldCount());
        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(leftTable, left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(rightTable, right.getTableScan());
    }

    @Test
    public void testInnerCorrelateTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);

        final Holder<RexCorrelVariable> v = Holder.of(null);
        RelNode joinTable = relBuilder
                .push(leftTable)
                .variable(v)
                .push(rightTable)
                .filter(
                        relBuilder.equals(relBuilder.field(0), relBuilder.field(v.get(), "username")))
                .correlate(
                        JoinRelType.INNER, v.get().id, relBuilder.field(2, 0, "username"))
                .build();
        System.out.println(joinTable);


        Assert.assertEquals(4, joinTable.getRowType().getFieldCount());
        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(leftTable, left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(rightTable, right.getTableScan());
    }

    @Test
    public void testSemiCorrelateTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);

        final Holder<RexCorrelVariable> v = Holder.of(null);
        RelNode joinTable = relBuilder
                .push(leftTable)
                .variable(v)
                .push(rightTable)
                .filter(
                        relBuilder.equals(relBuilder.field(0), relBuilder.field(v.get(), "username")))
                .correlate(
                        JoinRelType.SEMI, v.get().id, relBuilder.field(2, 0, "username"))
                .build();
        System.out.println(joinTable);


        Assert.assertEquals(2, joinTable.getRowType().getFieldCount());
        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(leftTable, left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(rightTable, right.getTableScan());
    }

    @Test
    public void testAntiCorrelateTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user(relBuilder);

        final Holder<RexCorrelVariable> v = Holder.of(null);
        RelNode joinTable = relBuilder
                .push(leftTable)
                .variable(v)
                .push(rightTable)
                .filter(
                        relBuilder.equals(relBuilder.field(0), relBuilder.field(v.get(), "username")))
                .correlate(
                        JoinRelType.ANTI, v.get().id, relBuilder.field(2, 0, "username"))
                .build();
        System.out.println(joinTable);


        Assert.assertEquals(2, joinTable.getRowType().getFieldCount());
        ColumnMapping2 columnMapping = new ColumnMapping2();
        joinTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(leftTable, left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(2);
        Assert.assertEquals(0, right.getIndex());
        Assert.assertEquals(rightTable, right.getTableScan());
    }

    @Test
    public void testUnionTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user2(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode unionTable = relBuilder.union(true).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        unionTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertNull(left);

    }

    @Test
    public void testUnionTable2() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(leftTable);

        RelNode unionTable = relBuilder.union(true).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        unionTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertNotNull(left);

    }

    @Test
    public void testIntersectTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user2(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode unionTable = relBuilder.intersect(true).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        unionTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertNull(left);

    }

    @Test
    public void testIntersectTable2() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(leftTable);

        RelNode unionTable = relBuilder.intersect(true).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        unionTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertNotNull(left);

    }

    @Test
    public void testMinusTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        RelNode rightTable = createTable_user2(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(rightTable);

        RelNode unionTable = relBuilder.minus(true).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        unionTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertNull(left);

    }

    @Test
    public void testMinusTable2() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode leftTable = createTable_user(relBuilder);
        relBuilder.push(leftTable);
        relBuilder.push(leftTable);

        RelNode unionTable = relBuilder.minus(true).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        unionTable.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertNotNull(left);

    }


    @Test
    public void testAggTable() {

        RelBuilder relBuilder = createRelBuilder();
        RelNode relNode = createTable_user(relBuilder);
        RelBuilder builder = relBuilder.push(relNode);
        relNode = builder.aggregate(builder.groupKey(0, 1)).build();
        ColumnMapping2 columnMapping = new ColumnMapping2();
        relNode.accept(columnMapping);

        ColumnInfo left = columnMapping.getBottomColumnInfo(0);
        Assert.assertEquals(0, left.getIndex());
        Assert.assertEquals(relNode.getInput(0), left.getTableScan());

        ColumnInfo right = columnMapping.getBottomColumnInfo(1);
        Assert.assertEquals(1, right.getIndex());
        Assert.assertEquals(relNode.getInput(0), right.getTableScan());
    }

    public static RelBuilder createRelBuilder() {
        RelOptCluster relOptCluster = DrdsRunner.newCluster();
        return MycatCalciteSupport.relBuilderFactory.create(relOptCluster, null);
    }
}