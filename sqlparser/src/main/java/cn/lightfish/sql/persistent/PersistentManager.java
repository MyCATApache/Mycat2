package cn.lightfish.sql.persistent;

import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sql.persistent.impl.DefaultPersistentProvider;
import cn.lightfish.sql.schema.MycatConsole;
import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum PersistentManager {
    INSTANCE;
    final ConcurrentMap<MycatTable, Persistent> map = new ConcurrentHashMap<>();
    final ConcurrentMap<String, PersistentProvider> persistentProviderMap = new ConcurrentHashMap<>();
    final DefaultPersistentProvider defaultPersistentProvider = new DefaultPersistentProvider();
    final String DEFAULT = "default";

    PersistentManager() {
        persistentProviderMap.put(DEFAULT, defaultPersistentProvider);
    }

    public void register(String name, PersistentProvider persistentProvider) {
        persistentProviderMap.put(name, persistentProvider);
    }

    public void createPersistent(MycatTable table, String persistentName,
                                 Map<String, Object> persistentAttributes) {
        persistentName = (persistentName == null) ? DEFAULT : persistentName;
        persistentAttributes =
                persistentAttributes == null ? Collections.emptyMap() : persistentAttributes;
        PersistentProvider persistentProvider = persistentProviderMap.get(persistentName);
        map.put(table, persistentProvider.create(table, persistentAttributes));
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

    public void assignmentUpdatePersistent(MycatConsole console, LogicLeafTableExecutor logicTableExecutor) {
        if (logicTableExecutor.physicsExecutor == null) {
            logicTableExecutor.setPhysicsExecutor(getUpdatePersistent(console, logicTableExecutor.getTable(),
                    logicTableExecutor.columnDefList(), logicTableExecutor.getPersistentAttribute()));
        }
    }

    public void assignmentQueryPersistent(MycatConsole console, LogicLeafTableExecutor logicTableExecutor) {
        if (logicTableExecutor.physicsExecutor == null) {
            logicTableExecutor.setPhysicsExecutor(getQueryPersistent(console, logicTableExecutor.getTable(),
                    logicTableExecutor.columnDefList(), logicTableExecutor.getPersistentAttribute()));
        }
    }

    public UpdatePersistent getUpdatePersistent(MycatConsole console, MycatTable table, TableColumnDefinition[] columnDefinition, Map<String, Object> emptyMap) {
        Persistent persistent = map.get(table);
        return persistent.createUpdatePersistent(columnDefinition, emptyMap);
    }
}