package io.mycat.calcite.logical;

import com.google.common.collect.Iterables;
import io.mycat.calcite.*;
import io.mycat.calcite.executor.ScanExecutor;
import io.mycat.mpp.Row;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;

import java.io.PrintWriter;
import java.io.StringWriter;

public class IndexTableView extends AbstractRelNode implements MycatRel {
    private final RelNode input;
    private final Iterable<Object[]> rows;

    public IndexTableView(RelNode input, Iterable<Object[]> rows) {
        super(input.getCluster(), input.getCluster().traitSetOf(MycatConvention.INSTANCE));
        this.input = input;
        this.rows = rows;
        this.rowType = input.getRowType();
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name(getClass().getName())
                .into();
        if (input instanceof MycatRel){
            ((MycatRel) input).explain(writer);
        }else {
            final RelWriterImpl pw =
                    new RelWriterImpl(new PrintWriter(new StringWriter()));
            input.explain(pw);
            writer.item("relNode",pw.toString());
        }
        return writer;
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return new ScanExecutor( Iterables.transform(rows,(i)-> Row.of(i)).iterator());
    }
}
