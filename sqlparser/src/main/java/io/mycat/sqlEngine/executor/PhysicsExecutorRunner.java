package io.mycat.sqlEngine.executor;

import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.executor.logicExecutor.DeleteExecutor;
import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.executor.logicExecutor.InsertExecutor;
import io.mycat.sqlEngine.executor.logicExecutor.LogicLeafTableExecutor;
import io.mycat.sqlEngine.persistent.PersistentManager;
import io.mycat.sqlEngine.persistent.UpdatePersistent;
import io.mycat.sqlEngine.schema.DbConsole;

import java.util.List;

public class PhysicsExecutorRunner {

    public Executor run(DbConsole console) {
        UpdatePersistent updatePersistent;
        List<LogicLeafTableExecutor> leafExecutor = console.getContext().getLeafExecutor();
        LogicLeafTableExecutor root = null;
        for (LogicLeafTableExecutor logicLeafTableExecutor : leafExecutor) {
            switch (logicLeafTableExecutor.getType()) {
                case QUERY:
                    PersistentManager.INSTANCE.assignmentQueryPersistent(console, logicLeafTableExecutor);
                    break;
                case UPDATE:
                case DELETE:
                    root =logicLeafTableExecutor;
                    break;
                case INSERT:
                    throw new UnsupportedOperationException();
            }
        }
        RootSessionContext context = console.getContext();
        switch (context.rootType) {
            case QUERY:
                return context.getQueryExecutor();
            case UPDATE:
                return context.getUpdateExecutor();
            case DELETE:
                DeleteExecutor deleteExecutor = context.getDeleteExecutor();
                UpdatePersistent updatePersistent1 = PersistentManager.INSTANCE.getUpdatePersistent(console, deleteExecutor.getTable(),
                        deleteExecutor.columnDefList(), deleteExecutor.getPersistentAttribute());
                root.setPhysicsExecutor(updatePersistent1);
                deleteExecutor.setUpdatePersistent(updatePersistent1);
                return context.getDeleteExecutor();
            case INSERT:
                InsertExecutor insertExecutor = context.getInsertExecutor();
                insertExecutor.setInsertPersistent(PersistentManager.INSTANCE
                        .getInsertPersistent(console,insertExecutor.getTable(),insertExecutor.getColumnNameList()
                        ,insertExecutor.getPersistentAttribute()));
                return insertExecutor;
            default:
            throw new UnsupportedOperationException();
        }
    }
}