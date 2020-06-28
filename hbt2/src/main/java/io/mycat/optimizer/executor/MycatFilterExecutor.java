package io.mycat.optimizer.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.optimizer.Executor;
import io.mycat.optimizer.Row;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.objenesis.instantiator.util.UnsafeUtils;

import java.util.function.Predicate;

public class MycatFilterExecutor implements Executor {
    private final Predicate<Row> predicate;
    private final Executor input;

    public MycatFilterExecutor(Predicate<Row> predicate, Executor input) {
        this.predicate = predicate;
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public Row next() {
        Row row;
        do {
            row = input.next();
            if (row == null) {
                input.close();
                return null;
            }
        } while (predicate.test(row) != Boolean.TRUE);
        return row;
    }

    @Override
    public void close() {
        input.close();
    }
}