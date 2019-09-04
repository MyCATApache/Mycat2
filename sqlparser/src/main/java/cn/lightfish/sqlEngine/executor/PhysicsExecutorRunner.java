package cn.lightfish.sqlEngine.executor;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.executor.logicExecutor.DeleteExecutor;
import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.executor.logicExecutor.InsertExecutor;
import cn.lightfish.sqlEngine.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sqlEngine.persistent.PersistentManager;
import cn.lightfish.sqlEngine.persistent.UpdatePersistent;
import cn.lightfish.sqlEngine.schema.DbConsole;

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