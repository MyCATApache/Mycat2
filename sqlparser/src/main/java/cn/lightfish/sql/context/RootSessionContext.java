package cn.lightfish.sql.context;

import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RootSessionContext {

  public Object[] scope = new Object[32];
  public final Map<String, Object> sessionVariants = new HashMap<>();
  public final List<LogicLeafTableExecutor> leafExecutor = new ArrayList<>();
  public Executor rootProject;

  public RootSessionContext() {
  }


  public void createTableSourceContext(int size) {
    scope = new Object[size];
    leafExecutor.clear();
  }

  public void setRootProject(Executor executor) {
    this.rootProject = executor;
  }

  public Object getGlobalVariant(String name) {
    return GlobalContext.INSTANCE.getGlobalVariant(name);
  }

  public Executor getRootProject() {
    return rootProject;
  }

  public Object getSessionVariant(String name) {
    return sessionVariants.get(name);
  }

  public List<LogicLeafTableExecutor> getLeafExecutor() {
    return leafExecutor;
  }
}