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
    public PrepareExecutor prepare(Plan plan) {
        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
        PrepareExecutor bindable = codeExecuterContext.bindable;
        if (bindable != null) return bindable;

        try {

        } catch (Exception exception) {
            LOGGER.error("", exception);

        }
        ArrayBindable bindable1 = getArrayBindable(codeExecuterContext);
        return PrepareExecutor.of(PrepareExecutorType.OBJECT, bindable1, plan.getMetaData());
    }

    @Override
    public RowBaseIterator runAsObjectArray(MycatDataContext context, String sqlStatement) {
        DrdsSqlWithParams drdsSql = DrdsRunnerHelper.preParse(sqlStatement, context.getDefaultSchema());
        Plan plan = DrdsRunnerHelper.getPlan(drdsSql);
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, plan.getCodeExecuterContext(), drdsSql);
        return prepare(plan).asRowBaseIterator(sqlMycatDataContext);

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
