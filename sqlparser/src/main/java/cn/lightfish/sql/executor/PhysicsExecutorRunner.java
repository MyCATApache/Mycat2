package cn.lightfish.sql.executor;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.executor.logicExecutor.DeleteExecutor;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.InsertExecutor;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sql.persistent.PersistentManager;
import cn.lightfish.sql.persistent.UpdatePersistent;
import cn.lightfish.sql.schema.MycatConsole;

import java.util.List;

public class PhysicsExecutorRunner {

    public Executor run(MycatConsole console) {
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