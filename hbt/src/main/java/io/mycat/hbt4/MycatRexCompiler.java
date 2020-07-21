package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.executor.MycatScalar;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class MycatRexCompiler {
    final static RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
    final static SqlConformance conformance = MycatCalciteSupport.INSTANCE.config.getParserConfig().conformance();

    final static boolean debug = true;
    final static RelDataType EmptyInputRowType = MycatCalciteSupport.INSTANCE.TypeFactory.builder().build();
    public static MycatScalar compile(List<RexNode> nodes, RelDataType inputRowType){
        return compile(nodes, inputRowType, a0 -> {
           throw new UnsupportedOperationException();
        });
    }
    public static MycatScalar compile(List<RexNode> nodes, RelDataType inputRowType,
                               Function1<String, RexToLixTranslator.InputGetter> inputGetterFunction) {
        if (inputRowType == null) inputRowType = EmptyInputRowType;
        final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }
        final RexProgram program = programBuilder.getProgram();

        final BlockBuilder builder = new BlockBuilder();
        final ParameterExpression context_ =
                Expressions.parameter(MycatContext.class, "context");
        final ParameterExpression outputValues_ =
                Expressions.parameter(Object[].class, "outputValues");
        final JavaTypeFactoryImpl javaTypeFactory =
                new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

        // public void execute(Context, Object[] outputValues)
        final RexToLixTranslator.InputGetter inputGetter =
                new RexToLixTranslator.InputGetterImpl(
                        ImmutableList.of(
                                Pair.of(
                                        Expressions.field(context_,
                                                MycatBuiltInMethod.CONTEXT_VALUES.field),
                                        PhysTypeImpl.of(javaTypeFactory, inputRowType,
                                                JavaRowFormat.ARRAY, false))));
        final Function1<String, RexToLixTranslator.InputGetter> correlates = inputGetterFunction;
        final Expression root = Expressions.parameter(MycatContext.class,"root");
        final List<Expression> list =
                RexToLixTranslator.translateProjects(program, javaTypeFactory,
                        conformance, builder, null, root, inputGetter, correlates);
        for (int i = 0; i < list.size(); i++) {
            builder.add(
                    Expressions.statement(
                            Expressions.assign(
                                    Expressions.arrayIndex(outputValues_,
                                            Expressions.constant(i)),
                                    list.get(i))));
        }
        return baz(context_, outputValues_, builder.toBlock());
    }

    /**
     * Given a method that implements {@link Scalar#execute(Context, Object[])},
     * adds a bridge method that implements {@link Scalar#execute(Context)}, and
     * compiles.
     */
    static MycatScalar baz(ParameterExpression context_,
                           ParameterExpression outputValues_, BlockStatement block) {
        final List<MemberDeclaration> declarations = new ArrayList<>();

        // public void execute(Context, Object[] outputValues)
        declarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, void.class,
                        MycatBuiltInMethod.SCALAR_EXECUTE2.method.getName(),
                        ImmutableList.of(context_, outputValues_), block));

        // public Object execute(Context)
        final BlockBuilder builder = new BlockBuilder();
        final Expression values_ = builder.append("values",
                Expressions.newArrayBounds(Object.class, 1,
                        Expressions.constant(1)));
        builder.add(
                Expressions.statement(
                        Expressions.call(
                                Expressions.parameter(MycatScalar.class, "this"),
                                MycatBuiltInMethod.SCALAR_EXECUTE2.method, context_, values_)));
        builder.add(
                Expressions.return_(null,
                        Expressions.arrayIndex(values_, Expressions.constant(0))));
        declarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, Object.class,
                        MycatBuiltInMethod.SCALAR_EXECUTE1.method.getName(),
                        ImmutableList.of(context_), builder.toBlock()));

        final ClassDeclaration classDeclaration =
                Expressions.classDecl(Modifier.PUBLIC, "Buzz", null,
                        ImmutableList.of(MycatScalar.class), declarations);
        String s = Expressions.toString(declarations, "\n", false);
        if (debug) {
            Util.debugCode(System.out, s);
        }
        try {
            return getScalar(classDeclaration, s);
        } catch (CompileException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    static MycatScalar getScalar(ClassDeclaration expr, String s)
            throws CompileException, IOException {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }
        IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName(expr.name);
        cbe.setImplementedInterfaces(new Class[]{MycatScalar.class});
        cbe.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());
        if (debug) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation(true, true, true);
        }
        return (MycatScalar) cbe.createInstance(new StringReader(s));
    }

}