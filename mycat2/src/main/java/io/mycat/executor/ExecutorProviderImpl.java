package io.mycat.executor;

import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.*;
import io.mycat.calcite.spm.Plan;
import io.ordinate.engine.builder.CalciteCompiler;
import io.ordinate.engine.builder.RexConverter;
import io.ordinate.engine.function.bind.BindVariable;
import io.ordinate.engine.function.bind.IndexedParameterLinkFunction;
import io.ordinate.engine.function.bind.SessionVariable;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import lombok.SneakyThrows;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.runtime.Utilities;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class ExecutorProviderImpl implements ExecutorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorProviderImpl.class);
    public static final ExecutorProviderImpl INSTANCE = new ExecutorProviderImpl();

    @Override
    public PrepareExecutor prepare(Plan plan) {
        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
        PrepareExecutor bindable = codeExecuterContext.bindable;
        if (bindable != null) return bindable;

        try {

            return PrepareExecutor.of((newMycatDataContext, mycatRowMetaData) -> {
                DrdsSqlWithParams drdsSql = newMycatDataContext.getDrdsSql();
                CalciteCompiler mycatCalciteCompiler = new CalciteCompiler();
                PhysicalPlan factory = mycatCalciteCompiler.convert(plan.getMycatRel());
                RexConverter rexConverter = mycatCalciteCompiler.getRexConverter();
                Map<Integer, IndexedParameterLinkFunction> indexedMap = rexConverter.getIndexedParameterLinkFunctionMap();
                List<Object> params = drdsSql.getParams();
                if (!indexedMap.isEmpty()) {
                    for (int i = 0; i < params.size(); i++) {
                        Object o = params.get(i);
                        IndexedParameterLinkFunction indexedParameterLinkFunction = indexedMap.get(i);
                        if (indexedParameterLinkFunction!=null){
                            BindVariable base = (BindVariable) indexedParameterLinkFunction.getBase();
                            base.setObject(o);
                        }
                    }
                }
                List<SessionVariable> sessionMap = rexConverter.getSessionVariableFunctionMap();
                for (SessionVariable sessionVariable : sessionMap) {
                    sessionVariable.setSession(newMycatDataContext.getContext());
                }
                AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext =
                        new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(newMycatDataContext.getContext(), plan.getCodeExecuterContext(), drdsSql);

                RootContext rootContext = new RootContext(sqlMycatDataContext);
                Observable<VectorSchemaRoot> schemaRootObservable = factory.execute(rootContext);
                return PrepareExecutor.ArrowObservable.of(mycatRowMetaData, schemaRootObservable);
            },
                    getArrayBindable(codeExecuterContext));
        } catch (Exception exception) {
            LOGGER.error("", exception);

        }
        return null;
    }

    @Override
    public RowBaseIterator runAsObjectArray(MycatDataContext context, String sqlStatement) {
        DrdsSqlWithParams drdsSql = DrdsRunnerHelper.preParse(sqlStatement, context.getDefaultSchema());
        Plan plan = DrdsRunnerHelper.getPlan(drdsSql);
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, plan.getCodeExecuterContext(), drdsSql);
        PrepareExecutor prepareExecutor = prepare(plan);
        PrepareExecutor.ArrowObservable arrowObservable = prepareExecutor.asObservableVector(sqlMycatDataContext, plan.getMetaData());
        Iterable<VectorSchemaRoot> vectorSchemaRoots = arrowObservable.getObservable().blockingIterable();
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
            int rowCount = vectorSchemaRoot.getRowCount();
            List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object[] rows = new Object[fieldVectors.size()];
                for (int columnIndex = 0; columnIndex < fieldVectors.size(); columnIndex++) {
                    FieldVector fieldVector = fieldVectors.get(columnIndex);
                    rows[columnIndex] = fieldVector.getObject(rowIndex);
                }
                resultSetBuilder.addObjectRowPayload(rows);
            }
        }

        return resultSetBuilder.build(plan.getMetaData());

    }

    @NotNull
    private ArrayBindable getArrayBindable(CodeExecuterContext codeExecuterContext) {
        ArrayBindable bindable1 = asObjectArray(getBindable(codeExecuterContext.getCodeContext()));
        return bindable1;
    }

    @SneakyThrows
    static ArrayBindable getBindable(CodeContext codeContext) {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }
        final IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName(codeContext.getName());
        cbe.setExtendedClass(Utilities.class);
        cbe.setImplementedInterfaces(new Class[]{ArrayBindable.class});
        cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation(true, true, true);
        }
        String code = codeContext.getCode();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(code);
        }
        return (ArrayBindable) cbe.createInstance(new StringReader(code));
    }

    @NotNull
    private static ArrayBindable asObjectArray(ArrayBindable bindable) {
        if (bindable.getElementType().isArray()) {
            return bindable;
        }
        return new ArrayBindable() {
            @Override
            public Class<Object[]> getElementType() {
                return Object[].class;
            }

            @Override
            public Enumerable<Object[]> bind(NewMycatDataContext dataContext) {
                Enumerable enumerable = bindable.bind(dataContext);
                return enumerable.select(e -> {
                    return new Object[]{e};
                });
            }
        };
    }

}
