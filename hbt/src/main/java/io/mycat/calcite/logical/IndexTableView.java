package io.mycat.calcite.logical;

import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.util.BuiltInMethod;

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.mycat.calcite.logical.MycatView.toEnumerable;
import static io.mycat.calcite.logical.MycatView.toRows;

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
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        Expression stash = implementor.stash(this, IndexTableView.class);
        final Expression iterable = toEnumerable(
                Expressions.call(stash, "getRows"));
        final Expression expression2 = toEnumerable(
                Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method,iterable));
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        builder.add(toRows(physType, expression2,getRowType().getFieldCount()));
        return implementor.result(physType, builder.toBlock());
    }

    public Iterable<Object[]> getRows() {
        return rows;
    }
}
