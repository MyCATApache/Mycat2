package io.mycat.calcite;

import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.spm.Plan;
import org.apache.calcite.runtime.ArrayBindable;

public interface ExecutorProvider {


    PrepareExecutor prepare(AsyncMycatDataContextImpl newMycatDataContext,
                            Plan plan);

    public  RowBaseIterator runAsObjectArray(MycatDataContext context, String sqlStatement);

    RowBaseIterator runAsObjectArray(AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext);
}
