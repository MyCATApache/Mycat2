package io.mycat.executor;

import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlObjectArrayRow;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.*;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.spm.Plan;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
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
import java.util.Iterator;

public class ExecutorProviderImpl implements ExecutorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorProviderImpl.class);
    public static final ExecutorProviderImpl INSTANCE = new ExecutorProviderImpl();

    @Override
    public PrepareExecutor prepare(AsyncMycatDataContextImpl newMycatDataContext,
                                   Plan plan) {
        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
        Object bindable = codeExecuterContext.bindable;
        if (bindable != null) return (PrepareExecutor) bindable;

        try {

        } catch (Exception exception) {
            LOGGER.error("", exception);

        }
        return getPrepareExecutor(newMycatDataContext, plan, codeExecuterContext);
    }

    @Override
    public RowBaseIterator runAsObjectArray(MycatDataContext context, String sqlStatement) {
        DrdsSqlWithParams drdsSql = DrdsRunnerHelper.preParse(sqlStatement, context.getDefaultSchema());
        Plan plan = DrdsRunnerHelper.getPlan(drdsSql);
        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
        ArrayBindable bindable1 = getArrayBindable(codeExecuterContext);
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, plan.getCodeExecuterContext(), drdsSql);
        MycatRowMetaData metaData = plan.getMetaData();
        Object bindObservable = bindable1.bindObservable(sqlMycatDataContext);
        Observable<Object[]> observable;
        if (bindObservable instanceof Observable) {
            observable = (Observable) bindObservable;
        } else {
            Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
            observable = toObservable(sqlMycatDataContext, enumerable);
        }
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        Iterator<Object[]> iterator = observable.blockingIterable().iterator();
        while (iterator.hasNext()){
            Object[] row = iterator.next();
            resultSetBuilder.addObjectRowPayload(row);
        }
        return resultSetBuilder.build(metaData);
    }

    @Override
    public RowBaseIterator runAsObjectArray(AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext) {
        CodeExecuterContext codeExecuterContext = sqlMycatDataContext.getCodeExecuterContext();
        MycatRel mycatRel = codeExecuterContext.getMycatRel();
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(mycatRel.getRowType().getFieldList());
        ArrayBindable bindable1 = getArrayBindable(codeExecuterContext);
        return new EnumeratorRowIterator(calciteRowMetaData, bindable1.bind(sqlMycatDataContext).enumerator());
    }

    @NotNull
    public PrepareExecutor getPrepareExecutor(AsyncMycatDataContextImpl newMycatDataContext, Plan plan, CodeExecuterContext codeExecuterContext) {
        ArrayBindable bindable1 = getArrayBindable(codeExecuterContext);
        Observable<MysqlPayloadObject> mysqlPayloadObjectObservable = getMysqlPayloadObjectObservable(bindable1, newMycatDataContext, plan);
        return PrepareExecutor.of(PrepareExecutorType.OBJECT, mysqlPayloadObjectObservable);
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

    @NotNull
    public static Observable<MysqlPayloadObject> getMysqlPayloadObjectObservable(
            ArrayBindable bindable,
            AsyncMycatDataContextImpl newMycatDataContext,
            Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = Observable.<MysqlPayloadObject>create(emitter -> {
            emitter.onNext(new MySQLColumnDef(plan.getMetaData()));
            try {

                Object bindObservable;
                bindObservable = bindable.bindObservable(newMycatDataContext);
                Observable<Object[]> observable;
                if (bindObservable instanceof Observable) {
                    observable = (Observable) bindObservable;
                } else {
                    Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
                    observable = toObservable(newMycatDataContext, enumerable);
                }
                observable.subscribe(objects -> emitter.onNext(new MysqlObjectArrayRow(objects)),
                        throwable -> {
                            newMycatDataContext.endFuture()
                                    .onComplete(event -> emitter.onError(throwable));
                        }, () -> {
                            CompositeFuture compositeFuture = newMycatDataContext.endFuture();
                            compositeFuture.onSuccess(event -> emitter.onComplete());
                            compositeFuture.onFailure(event -> emitter.onError(event));
                        });
            } catch (Throwable throwable) {
                CompositeFuture compositeFuture = newMycatDataContext.endFuture();
                compositeFuture.onComplete(event -> emitter.onError(throwable));
            }
        });
        return rowObservable;
    }

    @NotNull
    private static Observable<Object[]> toObservable(AsyncMycatDataContextImpl context, Enumerable<Object[]> enumerable) {
        Observable<Object[]> observable;
        observable = Observable.create(emitter1 -> {
            Future future;
            try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
                while (enumerator.moveNext()) {
                    emitter1.onNext(enumerator.current());
                }
                future = Future.succeededFuture();
            } catch (Throwable throwable) {
                future = Future.failedFuture(throwable);
            }
            CompositeFuture.join(future, context.endFuture())
                    .onSuccess(event -> emitter1.onComplete())
                    .onFailure(event -> emitter1.onError(event));
        });
        return observable;
    }
}
