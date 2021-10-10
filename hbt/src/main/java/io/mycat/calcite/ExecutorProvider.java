package io.mycat.calcite;

import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.calcite.spm.Plan;
import org.apache.calcite.runtime.ArrayBindable;

public interface ExecutorProvider {


    PrepareExecutor prepare(AsyncMycatDataContextImpl newMycatDataContext,
                            Plan plan);

    public ArrayBindable prepare(CodeExecuterContext codeExecuterContext);
}
