package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import java.util.List;
import java.util.stream.Collectors;

public class MycatGatherExecutor implements Executor {
    final List<Executor> executors;
    private Enumerator<Row> iter;

    protected MycatGatherExecutor(List<Executor> executors) {
        this.executors = executors;
    }

    public static MycatGatherExecutor create(List<Executor> executors) {
        return new MycatGatherExecutor(executors);
    }

    @Override
    public void open() {
        this.iter = Linq4j.concat(executors.stream().parallel().map(i -> {
            i.open();
            return Linq4j.asEnumerable(i);
        }).collect(Collectors.toList())).enumerator();
    }

    @Override
    public Row next() {
        if(iter.moveNext()){
            return iter.current();
        }
        return null;
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
        return executors.stream().allMatch(Executor::isRewindSupported);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        for (Executor executor : executors) {
            executor.explain(explainWriter);
        }
        return explainWriter.ret();
    }
}