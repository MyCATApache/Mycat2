package io.mycat.calcite.executor.aggfunction;

import io.mycat.calcite.executor.MycatScalar;
import io.mycat.mpp.Row;

/**
 * Accumulator powered by {@link MycatScalar} code fragments.
 */
public class ScalarAccumulator implements Accumulator {
    final ScalarAccumulatorDef def;
    final Object[] values;

    public ScalarAccumulator(ScalarAccumulatorDef def, Object[] values) {
        this.def = def;
        this.values = values;
    }

    public void send(Row row) {
        System.arraycopy(row.getValues(), 0, def.sendContext.values, 0,
                def.rowLength);
        System.arraycopy(values, 0, def.sendContext.values, def.rowLength,
                values.length);
        def.addScalar.execute(def.sendContext, values);
    }

    public Object end() {
        System.arraycopy(values, 0, def.endContext.values, 0, values.length);
        return def.endScalar.execute(def.endContext);
    }
}