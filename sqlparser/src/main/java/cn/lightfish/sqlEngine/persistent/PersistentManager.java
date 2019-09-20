/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sqlEngine.persistent.impl.DefaultPersistentProvider;
import cn.lightfish.sqlEngine.schema.DbConsole;
import cn.lightfish.sqlEngine.schema.DbTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
/**
 * @author Junwen Chen
 **/
public enum PersistentManager {
    INSTANCE;
    final ConcurrentMap<DbTable, Persistent> map = new ConcurrentHashMap<>();
    final ConcurrentMap<String, PersistentProvider> persistentProviderMap = new ConcurrentHashMap<>();
    final DefaultPersistentProvider defaultPersistentProvider = new DefaultPersistentProvider();
    final String DEFAULT = "default";

    PersistentManager() {
        persistentProviderMap.put(DEFAULT, defaultPersistentProvider);
    }

    public void register(String name, PersistentProvider persistentProvider) {
        persistentProviderMap.put(name, persistentProvider);
    }

    public void createPersistent(DbTable table, String persistentName,
                                 Map<String, Object> persistentAttributes) {
        persistentName = (persistentName == null) ? DEFAULT : persistentName;
        persistentAttributes = persistentAttributes == null ? Collections.emptyMap() : persistentAttributes;
        PersistentProvider persistentProvider = persistentProviderMap.get(persistentName);
        map.put(table, persistentProvider.create(table, persistentAttributes));
    }

    public InsertPersistent getInsertPersistent(DbConsole console,
                                                DbTable table, TableColumnDefinition[] columnNameList,
                                                Map<String, Object> persistentAttributes) {
        Persistent persistent = map.get(table);
        return persistent.createInsertPersistent(columnNameList, persistentAttributes);
    }

    public QueryPersistent getQueryPersistent(DbConsole console,
                                              DbTable table, TableColumnDefinition[] columnNameList,
                                              Map<String, Object> persistentAttributes) {
        Persistent persistent = map.get(table);
        return persistent.createQueryPersistent(columnNameList, persistentAttributes);
    }

    public void assignmentUpdatePersistent(DbConsole console, LogicLeafTableExecutor logicTableExecutor) {
        if (logicTableExecutor.physicsExecutor == null) {
            logicTableExecutor.setPhysicsExecutor(getUpdatePersistent(console, logicTableExecutor.getTable(),
                    logicTableExecutor.columnDefList(), logicTableExecutor.getPersistentAttribute()));
        }
    }

    public void assignmentQueryPersistent(DbConsole console, LogicLeafTableExecutor logicTableExecutor) {
        if (logicTableExecutor.physicsExecutor == null) {
            logicTableExecutor.setPhysicsExecutor(getQueryPersistent(console, logicTableExecutor.getTable(),
                    logicTableExecutor.columnDefList(), logicTableExecutor.getPersistentAttribute()));
        }
    }

    public UpdatePersistent getUpdatePersistent(DbConsole console, DbTable table, TableColumnDefinition[] columnDefinition, Map<String, Object> emptyMap) {
        Persistent persistent = map.get(table);
        return persistent.createUpdatePersistent(columnDefinition, emptyMap);
    }
}