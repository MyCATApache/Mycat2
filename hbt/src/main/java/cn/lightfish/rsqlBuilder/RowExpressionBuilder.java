package cn.lightfish.rsqlBuilder;

import cn.lightfish.describer.*;
import cn.lightfish.describer.literal.*;
import cn.lightfish.rsqlBuilder.schema.SchemaMatcher;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.Map;

public class RowExpressionBuilder {
    private final RelBuilder relBuilder;
    private final RexBuilder rexBuilder;
    private final RelDataTypeFactory typeFactory;
    private final RelOptCluster cluster;
    private final RelOptSchema relOptSchema;
    private final RelFactories.TableScanFactory scanFactory;

    public RowExpressionBuilder() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("foodmart", new ReflectiveSchema(new Db1()));
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema).build();
        this.relBuilder = RelBuilder.create(config);
        this.rexBuilder = relBuilder.getRexBuilder();
        this.typeFactory = relBuilder.getTypeFactory();
        this.cluster = relBuilder.getCluster();
        this.relOptSchema = relBuilder.getRelOptSchema();
        this.scanFactory = relBuilder.getScanFactory();

        RelNode build = RelBuilder.create(config)
                .scan("foodmart", "travelrecord")
                .filter()
                .build();

        Describer describer = new Describer("(let t = foodmart.travelrecord).project(t.id as id)");
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", "id");

        //////////////////////
        NameBuilder rexBuilder = getRexBuilder(describer, schemaMatcher);
        ParseNode stack = rexBuilder.getStack();
        RelBuilder relBuilder = RelBuilder.create(config);
        stack.accept(new ParseNodeVisitor() {
            @Override
            public void visit(Bind bind) {

            }

            @Override
            public void endVisit(Bind bind) {

            }

            @Override
            public void visit(CallExpr call) {
                ParenthesesExpr args = call.getArgs();
                args.accept(this);
            }

            @Override
            public void endVisit(CallExpr call) {

            }

            @Override
            public void visit(IdLiteral id) {

            }

            @Override
            public void endVisit(IdLiteral id) {

            }

            @Override
            public void visit(ParenthesesExpr parenthesesExpr) {
                for (ParseNode expr : parenthesesExpr.getExprs()) {
                    expr.accept(this);
                }

            }

            @Override
            public void endVisit(ParenthesesExpr parenthesesExpr) {

            }

            @Override
            public void visit(IntegerLiteral numberLiteral) {

            }

            @Override
            public void endVisit(IntegerLiteral numberLiteral) {

            }

            @Override
            public void visit(StringLiteral stringLiteral) {

            }

            @Override
            public void endVisit(StringLiteral stringLiteral) {

            }

            @Override
            public void visit(DecimalLiteral decimalLiteral) {

            }

            @Override
            public void endVisit(DecimalLiteral decimalLiteral) {

            }

            @Override
            public void visit(PropertyLiteral propertyLiteral) {

            }

            @Override
            public void endVisit(PropertyLiteral propertyLiteral) {

            }
        });

    }

    public static NameBuilder getRexBuilder(Describer describer, SchemaMatcher schemaMatcher) {
        ParseNode primary = describer.expression();
        Map<String, ParseNode> variables = describer.getVariables();

        variables.entrySet().forEach(stringNodeEntry -> stringNodeEntry.setValue(processDotCall(stringNodeEntry.getValue())));
        primary = processDotCall(primary);

        variables.entrySet().forEach(c -> {
            NameBuilder rexBuilder = new NameBuilder(schemaMatcher, variables);
            c.getValue().accept(rexBuilder);
            c.setValue(rexBuilder.getStack());
        });
        NameBuilder rexBuilder = new NameBuilder(schemaMatcher, variables);
        primary.accept(rexBuilder);
        return rexBuilder;
    }

    private static ParseNode processDotCall(ParseNode primary) {
        DotCallResolver callResolver = new DotCallResolver();
        primary.accept(callResolver);
        primary = callResolver.getStack();
        return primary;
    }

    public static void main(String[] args) {

        RowExpressionBuilder builder = new RowExpressionBuilder();


        System.out.println();
    }

    private Object field(String foodmart, String sales_fact_1997, String cust_id) {
        return null;
    }

    private RowExpr call(SqlOperator operator, RexNode... operands) {
        return null;
    }

    private RowExpressionBuilder from(String foodmart, String sales_fact_1997) {
        return null;
    }

    private void filter() {

    }

    private enum Op {
        EQUALS,

    }



    //
//    private RexNode makeLiteral(Object value) {
//        RexNode rexNode;
//        if (value == null) {
//            rexNode = builder.makeLiteral(
//                    null,
//                    typeFactory.createSqlType(SqlTypeName.NULL), true);
//        } else {
//            rexNode = builder.makeLiteral(value, typeFactory.createType(value.getClass()), true);
//        }
//        return rexNode;
//    }
    public static class Travelrecord {
        public final int id;
        public final int user_id;

        public Travelrecord(int cust_id, int prod_id) {
            this.id = cust_id;
            this.user_id = prod_id;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || obj instanceof Travelrecord
                    && id == ((Travelrecord) obj).id
                    && user_id == ((Travelrecord) obj).user_id;
        }
    }
}