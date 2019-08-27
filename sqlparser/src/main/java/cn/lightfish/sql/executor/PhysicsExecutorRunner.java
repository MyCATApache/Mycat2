package cn.lightfish.sql.executor;

import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sql.persistent.PersistentManager;
import cn.lightfish.sql.schema.MycatConsole;

public class PhysicsExecutorRunner {

  public Executor run(MycatConsole console) {
    for (LogicLeafTableExecutor logicTableExecutor : console.getContext().getLeafExecutor()) {
      PersistentManager.INSTANCE.assignmentQueryPersistent(console, logicTableExecutor);
    }
    return console.getContext().getRootProject();
  }
}