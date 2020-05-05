package io.mycat.commands;

import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.TableCollector;
import io.mycat.calcite.prepare.MycatTextUpdatePrepareObject;
import io.mycat.client.MycatRequest;
import io.mycat.metadata.LogicTableType;
import io.mycat.metadata.TableHandler;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public enum ExecuteCommand implements MycatCommand {
    INSTANCE;
    final static Logger logger = LoggerFactory.getLogger(ExecuteCommand.class);

    /*
     * balance
     * targets
     * executeType:
     * metaData:true:false
     * forceProxy:true:false
     * needTransaction:true|false
     */
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        String balanceConfig = request.getOrDefault("balance", null);
        String targetsConfig = request.getOrDefault("targets", null);
        boolean needTransaction = Boolean.TRUE.toString().equalsIgnoreCase(request.getOrDefault("needTransaction", Boolean.TRUE.toString()));
        boolean forceProxy = Boolean.TRUE.toString().equalsIgnoreCase(request.getOrDefault("forceProxy", Boolean.FALSE.toString()));
        boolean metaData = Boolean.TRUE.toString().equalsIgnoreCase(request.getOrDefault("metaData", Boolean.FALSE.toString()));
        ExecuteType executeType = ExecuteType.valueOf(request.getOrDefault("executeType", ExecuteType.DEFAULT.name()));
        ExplainDetail detail = getDetails(metaData, targetsConfig, context, balanceConfig, request.getText(), executeType, forceProxy,needTransaction);
        response.execute(detail);
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        String balanceConfig = request.getOrDefault("balance", null);
        String targetsConfig = request.getOrDefault("targets", null);
        boolean needTransaction = Boolean.TRUE.toString().equalsIgnoreCase(request.getOrDefault("needTransaction", Boolean.TRUE.toString()));
        boolean forceProxy = Boolean.TRUE.toString().equalsIgnoreCase(request.getOrDefault("forceProxy", Boolean.FALSE.toString()));
        boolean metaData = Boolean.TRUE.toString().equalsIgnoreCase(request.getOrDefault("metaData", Boolean.FALSE.toString()));
        ExecuteType executeType = ExecuteType.valueOf(request.getOrDefault("executeType", ExecuteType.DEFAULT.name()));
        ExplainDetail detail = getDetails(metaData, targetsConfig, context, balanceConfig, request.getText(), executeType, forceProxy,needTransaction);
        response.sendExplain(ExecuteCommand.class, detail);
        return true;
    }

    @Override
    public String getName() {
        return "execute";
    }

    public static ExplainDetail getDetails(MycatRequest request, MycatDataContext context, ExecuteType executeType) {
        return getDetails(true, null, context, null, request.getText(), executeType, false);

    }
    public static ExplainDetail getDetails(boolean metaData,
                                           String targetsConfig,
                                           MycatDataContext context,
                                           String balance,
                                           String sql,
                                           ExecuteType executeType, boolean forceProxy){
        return getDetails(metaData, targetsConfig, context, balance, sql, executeType, forceProxy,true);
    }
    public static ExplainDetail getDetails(boolean metaData,
                                           String targetsConfig,
                                           MycatDataContext context,
                                           String balance,
                                           String sql,
                                           ExecuteType executeType, boolean forceProxy,boolean needTransaction) {
        boolean needStartTransaction = needTransaction && (!context.isAutocommit() || context.isInTransaction());
        if (metaData) {
            Map<String, Collection<String>> tableMap = TableCollector.collect(context.getDefaultSchema(), sql);
            Iterator<Map.Entry<String, Collection<String>>> iterator = tableMap.entrySet().iterator();
            if (!iterator.hasNext()) {
                throw new IllegalArgumentException("无法识别表名:" + sql);
            }
            Map.Entry<String, Collection<String>> entry = iterator.next();
            String schemaName = entry.getKey();
            String tableName = entry.getValue().iterator().next();
            tableMap = null;//help gc

            MycatDBClientMediator mycatDb = MycatDBs.createClient(context);
            TableHandler tableHandler = mycatDb.config().getTable(schemaName, tableName);
            boolean isGlobal = tableHandler.getType() == LogicTableType.GLOBAL;
            boolean master = executeType != ExecuteType.QUERY || needStartTransaction || executeType != null && executeType.isMaster()||context.isInTransaction();
            MycatTextUpdatePrepareObject mycatTextUpdatePrepareObject = mycatDb.getUponDBSharedServer().innerUpdatePrepareObject(sql, mycatDb);
            Map<String, List<String>> routeMap = mycatTextUpdatePrepareObject.getRouteMap();
            return ExplainDetail.builder()
                    .executeType(executeType)
                    .targets(resolveDataSourceName(balance, master, routeMap))
                    .globalTableUpdate(isGlobal)
                    .forceProxy(forceProxy)
                    .needStartTransaction(needStartTransaction)
                    .build();
        } else {
            String replicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(
                    Objects.requireNonNull(targetsConfig, "can not get " + targetsConfig + " of " + "targets"),
                    needStartTransaction || executeType.isMaster()||context.isInTransaction(), balance);
            return ExplainDetail.builder()
                    .executeType(executeType)
                    .targets(Collections.singletonMap(replicaName, Collections.singletonList(sql)))
                    .balance(balance)
                    .forceProxy(forceProxy)
                    .needStartTransaction(needStartTransaction)
                    .build();
        }
    }

    @NotNull
    private static HashMap<String, List<String>> resolveDataSourceName(String balance, boolean master, Map<String, List<String>> routeMap) {
        HashMap<String, List<String>> map = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : routeMap.entrySet()) {
            String datasourceNameByReplicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey(), master, balance);
            List<String> list = map.computeIfAbsent(datasourceNameByReplicaName, s -> new ArrayList<>(1));
            list.addAll(entry.getValue());
        }
        return map;
    }

}