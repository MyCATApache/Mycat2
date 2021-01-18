/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.physical;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.*;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.*;

/**
 * Calc operator implemented in Mycat convention.
 */
public class MycatCalc extends Calc implements MycatRel {
    private final static Logger LOGGER = LoggerFactory.getLogger(MycatCalc.class);

    private final RexProgram program;

    protected MycatCalc(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        RexProgram program) {
        super(cluster, traitSet, input, program);
        assert getConvention() instanceof MycatConvention;
        this.program = program;
        this.rowType = program.getOutputRowType();
    }

    public static MycatCalc create(
            RelTraitSet traitSet,
            RelNode input,
            RexProgram program) {
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.calc(mq, input, program));
        return new MycatCalc(
                cluster,
                traitSet,
                input,
                program
        );
    }

    public RelWriter explainTerms(RelWriter pw) {
        return program.explainCalc(super.explainTerms(pw));
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double dRows = mq.getRowCount(this);
        double dCpu = mq.getRowCount(getInput())
                * program.getExprCount();
        double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }


    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new MycatCalc(getCluster(), traitSet, getInput(), program);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatCalc").item("program", this.program).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    public RexProgram getProgram() {
        return program;
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableRel child = (EnumerableRel) getInput();

        final Result result =
                implementor.visitChild(this, 0, child, pref);

        final PhysType physType =
                PhysTypeImpl.of(
                        typeFactory, getRowType(), pref.prefer(result.format));

        // final Enumerable<Employee> inputEnumerable = <<child adapter>>;
        // return new Enumerable<IntString>() {
        //     Enumerator<IntString> enumerator() {
        //         return new Enumerator<IntString>() {
        //             public void reset() {
        // ...
        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType =
                Types.of(
                        Enumerator.class, outputJavaType);
        Type inputJavaType = result.physType.getJavaRowType();
        ParameterExpression inputEnumerator =
                Expressions.parameter(
                        Types.of(
                                Enumerator.class, inputJavaType),
                        "inputEnumerator");
        Expression input =
                EnumUtils.convert(
                        Expressions.call(
                                inputEnumerator,
                                BuiltInMethod.ENUMERATOR_CURRENT.method),
                        inputJavaType);
        if (!input.getType().equals(inputJavaType)) {
            input = Expressions.convert_(input, inputJavaType);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("cast {} to {} fail", input, inputJavaType);
            }
        }

        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RelMetadataQuery mq = getCluster().getMetadataQuery();
        final RelOptPredicateList predicates = mq.getPulledUpPredicates(child);
        final RexSimplify simplify =
                new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR);
        final RexProgram program = this.program.normalize(rexBuilder, simplify);

        BlockStatement moveNextBody;
        if (program.getCondition() == null) {
            moveNextBody =
                    Blocks.toFunctionBlock(
                            Expressions.call(
                                    inputEnumerator,
                                    BuiltInMethod.ENUMERATOR_MOVE_NEXT.method));
        } else {
            final BlockBuilder builder2 = new BlockBuilder();
            Expression condition =
                    RexToLixTranslator.translateCondition(
                            program,
                            typeFactory,
                            builder2,
                            new RexToLixTranslator.InputGetterImpl(
                                    Collections.singletonList(
                                            Pair.of(input, result.physType))),
                            implementor.getAllCorrelateVariablesFunction(), implementor.getConformance());
            builder2.add(
                    Expressions.ifThen(
                            condition,
                            Expressions.return_(
                                    null, Expressions.constant(true))));
            moveNextBody =
                    Expressions.block(
                            Expressions.while_(
                                    Expressions.call(
                                            inputEnumerator,
                                            BuiltInMethod.ENUMERATOR_MOVE_NEXT.method),
                                    builder2.toBlock()),
                            Expressions.return_(
                                    null,
                                    Expressions.constant(false)));
        }

        final BlockBuilder builder3 = new BlockBuilder();
        final SqlConformance conformance =
                (SqlConformance) implementor.map.getOrDefault("_conformance",
                        SqlConformanceEnum.DEFAULT);
        List<Expression> expressions =
                RexToLixTranslator.translateProjects(
                        program,
                        typeFactory,
                        conformance,
                        builder3,
                        physType,
                        DataContext.ROOT,
                        new RexToLixTranslator.InputGetterImpl(
                                Collections.singletonList(
                                        Pair.of(input, result.physType))),
                        implementor.getAllCorrelateVariablesFunction());
        builder3.add(
                Expressions.return_(
                        null, physType.record(expressions)));
        BlockStatement currentBody =
                builder3.toBlock();

        final Expression inputEnumerable =
                builder.append(
                        "inputEnumerable", result.block, false);
        final Expression body =
                Expressions.new_(
                        enumeratorType,
                        NO_EXPRS,
                        Expressions.list(
                                Expressions.fieldDecl(
                                        Modifier.PUBLIC
                                                | Modifier.FINAL,
                                        inputEnumerator,
                                        Expressions.call(
                                                inputEnumerable,
                                                BuiltInMethod.ENUMERABLE_ENUMERATOR.method)),
                                EnumUtils.overridingMethodDecl(
                                        BuiltInMethod.ENUMERATOR_RESET.method,
                                        NO_PARAMS,
                                        Blocks.toFunctionBlock(
                                                Expressions.call(
                                                        inputEnumerator,
                                                        BuiltInMethod.ENUMERATOR_RESET.method))),
                                EnumUtils.overridingMethodDecl(
                                        BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                                        NO_PARAMS,
                                        moveNextBody),
                                EnumUtils.overridingMethodDecl(
                                        BuiltInMethod.ENUMERATOR_CLOSE.method,
                                        NO_PARAMS,
                                        Blocks.toFunctionBlock(
                                                Expressions.call(
                                                        inputEnumerator,
                                                        BuiltInMethod.ENUMERATOR_CLOSE.method))),
                                Expressions.methodDecl(
                                        Modifier.PUBLIC,
                                        BRIDGE_METHODS
                                                ? Object.class
                                                : outputJavaType,
                                        "current",
                                        NO_PARAMS,
                                        currentBody)));
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                // TODO: generics
                                //   Collections.singletonList(inputRowType),
                                NO_EXPRS,
                                ImmutableList.<MemberDeclaration>of(
                                        Expressions.methodDecl(
                                                Modifier.PUBLIC,
                                                enumeratorType,
                                                BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(),
                                                NO_PARAMS,
                                                Blocks.toFunctionBlock(body))))));
        return implementor.result(physType, builder.toBlock());
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        String correlVariable = this.getCorrelVariable();
        if (implementor.isStream() && correlVariable == null) {
            return implement(implementor, pref);
        } else {
            return implement(implementor, pref);
        }
    }

    @Override
    public boolean isSupportStream() {
        return this.getCorrelVariable() == null;
    }
}
