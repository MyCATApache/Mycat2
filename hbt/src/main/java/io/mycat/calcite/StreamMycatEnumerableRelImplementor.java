package io.mycat.calcite;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.RxBuiltInMethod;

import java.lang.reflect.Modifier;
import java.util.*;

public class StreamMycatEnumerableRelImplementor extends MycatEnumerableRelImplementor{

    public StreamMycatEnumerableRelImplementor(Map<String, Object> internalParameters) {
        super(internalParameters);
    }

    public ClassDeclaration implementHybridRoot(MycatRel rootRel, EnumerableRel.Prefer prefer) {
        EnumerableRel.Result result;
        try {
            if (rootRel.isSupportStream()){
                result = rootRel.implementStream(this,prefer);
            }else {
                result = rootRel.implement(this, prefer);
            }

        } catch (RuntimeException e) {
            IllegalStateException ex = new IllegalStateException("Unable to implement "
                    + RelOptUtil.toString(rootRel, SqlExplainLevel.ALL_ATTRIBUTES));
            ex.addSuppressed(e);
            throw ex;
        }
        final List<MemberDeclaration> memberDeclarations = new ArrayList<>();
        new TypeRegistrar(memberDeclarations).go(result);

        // This creates the following code
        // final Integer v1stashed = (Integer) root.get("v1stashed")
        // It is convenient for passing non-literal "compile-time" constants
        final Collection<Statement> stashed =
                Collections2.transform(stashedParameters.values(),
                        input -> Expressions.declare(Modifier.FINAL, input,
                                Expressions.convert_(
                                        Expressions.call(DataContext.ROOT,
                                                BuiltInMethod.DATA_CONTEXT_GET.method,
                                                Expressions.constant(input.name)),
                                        input.type)));

        final BlockStatement block = Expressions.block(
                Iterables.concat(
                        stashed,
                        result.block.statements));
        memberDeclarations.add(
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                       Observable.class,
                        "bindObservable",
                        Expressions.list(DataContext.ROOT),
                        block));
        memberDeclarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, boolean.class,
                        "isObservable",
                        ImmutableList.of(),
                        Blocks.toFunctionBlock(
                                Expressions.return_(null,
                                        Expressions.constant(true)))));
        memberDeclarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, Class.class,
                        BuiltInMethod.TYPED_GET_ELEMENT_TYPE.method.getName(),
                        ImmutableList.of(),
                        Blocks.toFunctionBlock(
                                Expressions.return_(null,
                                        Expressions.constant(result.physType.getJavaRowType())))));
        return Expressions.classDecl(Modifier.PUBLIC,
                "Baz",
                null,
                Collections.singletonList(Bindable.class),
                memberDeclarations);
    }

    @Override
    public EnumerableRel.Result visitChild(EnumerableRel parent, int ordinal, EnumerableRel child, EnumerableRel.Prefer prefer) {
        MycatRel rootRel = (MycatRel) parent;
        MycatRel childRel = (MycatRel) child;
        if (childRel.isSupportStream()){
         return childRel.implementStream(this,prefer);
        }
        return super.visitChild(parent, ordinal, child, prefer);
    }
}
