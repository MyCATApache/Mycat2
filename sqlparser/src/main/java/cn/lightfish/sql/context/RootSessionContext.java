package cn.lightfish.sql.context;

import cn.lightfish.sql.executor.logicExecutor.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RootSessionContext {

  public Object[] scope = new Object[32];
  public final Map<String, Object> sessionVariants = new HashMap<>();
  public final List<LogicLeafTableExecutor> leafExecutor = new ArrayList<>();
  public Executor queryExecutor;
  public InsertExecutor insertExecutor;
  public DeleteExecutor deleteExecutor;
  public UpdateExecutor updateExecutor;
  public ExecutorType rootType = ExecutorType.QUERY;

  public RootSessionContext() {
  }


  public void createTableSourceContext(int size) {
    scope = new Object[size];
    leafExecutor.clear();
  }

  public void setQueryExecutor(Executor executor) {
    this.queryExecutor = executor;
  }

  public Object getGlobalVariant(String name) {
    return GlobalContext.INSTANCE.getGlobalVariant(name);
  }

  public Executor getQueryExecutor() {
    return queryExecutor;
  }

  public Object getSessionVariant(String name) {
    return sessionVariants.get(name);
  }

  public List<LogicLeafTableExecutor> getLeafExecutor() {
    return leafExecutor;
  }

  public void setInsertExecutor(InsertExecutor insertExecutor) {
    this.insertExecutor = insertExecutor;
  }

  public InsertExecutor getInsertExecutor() {
    return insertExecutor;
  }

  public DeleteExecutor getDeleteExecutor() {
    return deleteExecutor;
  }

  public void setDeleteExecutor(DeleteExecutor deleteExecutor) {
    this.deleteExecutor = deleteExecutor;
  }

  public UpdateExecutor getUpdateExecutor() {
    return updateExecutor;
  }

  public void setUpdateExecutor(UpdateExecutor updateExecutor) {
    this.updateExecutor = updateExecutor;
  }
}