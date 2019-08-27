package cn.lightfish.sql.persistent;

import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sql.persistent.impl.DefaultPersistentProvider;
import cn.lightfish.sql.schema.MycatConsole;
import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum PersistentManager {
  INSTANCE;
  final ConcurrentMap<MycatTable, Persistent> map = new ConcurrentHashMap<>();
  final ConcurrentMap<String, PersistentProvider> persistentProviderMap = new ConcurrentHashMap<>();
  final DefaultPersistentProvider defaultPersistentProvider = new DefaultPersistentProvider();

  PersistentManager() {
    persistentProviderMap.put("default",defaultPersistentProvider);
  }

  public void register(String name,PersistentProvider persistentProvider){
    persistentProviderMap.put(name,persistentProvider);
  }

  public void createPersistent(MycatTable table, String persistentName,
      Map<String, Object> persistentAttributes) {
    PersistentProvider persistentProvider = persistentProviderMap.get(persistentName);
    map.put(table, persistentProvider.create(table, persistentAttributes));
  }

  public UpdatePersistent getUpdatePersistent(MycatConsole console,
      MycatTable table, TableColumnDefinition[] columnNameList, Map<String, Object> persistentAttributes) {
    Persistent persistent = map.get(table);
    return persistent.createUpdatePersistent(columnNameList, persistentAttributes);
  }
  public InsertPersistent getInsertPersistent(MycatConsole console,
      MycatTable table, TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes) {
    Persistent persistent = map.get(table);
    return persistent.createInsertPersistent(columnNameList, persistentAttributes);
  }

  public QueryPersistent getQueryPersistent(MycatConsole console,
      MycatTable table, TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes) {
    Persistent persistent = map.get(table);
    return persistent.createQueryPersistent(columnNameList, persistentAttributes);
  }
  public void assignmentQueryPersistent(MycatConsole console){
    List<LogicLeafTableExecutor> leafExecutor = console.getContext().getLeafExecutor();
    for (LogicLeafTableExecutor executor : leafExecutor) {

    }

  }
  public void assignmentQueryPersistent(MycatConsole console,
      LogicLeafTableExecutor logicTableExecutor) {
    logicTableExecutor.setPhysicsExecutor(getQueryPersistent(console, logicTableExecutor.getTable(), logicTableExecutor.columnDefList(),logicTableExecutor.getPersistentAttribute()));
  }
}