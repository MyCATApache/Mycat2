package io.mycat.router.custom;

import com.google.common.collect.ImmutableSet;
import io.mycat.*;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.function.IndexDataNode;
import io.mycat.util.JsonUtil;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class HttpCustomRuleFunction extends CustomRuleFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpCustomRuleFunction.class);
    Map<String, Object> properties;

    List<Partition> allScanPartitions;

    ScheduledFuture<?> scheduledFuture;
    private String name;
    Set<String> shardingDbKeys;
    Set<String> shardingTableKeys;
    Set<String> shardingTargetKeys;
    String erUniqueID;
    ShardingTableType shardingTableType;
    int requireShardingKeyCount;
    Set<String> requireShardingKeys;

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {
        this.properties = properties;
        this.name = (String) properties.get("name");
        this.shardingDbKeys = new HashSet<>(Arrays.asList(Objects.toString(properties.getOrDefault("shardingDbKeys","")).split(",")));
        this.shardingTableKeys = new HashSet<>(Arrays.asList(Objects.toString(properties.getOrDefault("shardingTableKeys","")).split(",")));
        this.shardingTargetKeys = new HashSet<>(Arrays.asList(Objects.toString(properties.getOrDefault("shardingTargetKeys","")).split(",")));
        this.erUniqueID = properties.toString();

        this.shardingTableType = ShardingTableType.computeByName(this.allScanPartitions);
        this.requireShardingKeys = (Set) ImmutableSet.builder()
                .addAll(this.shardingDbKeys)
                .addAll(this.shardingTableKeys)
                .addAll(this.shardingTargetKeys)
                .build();
        this.requireShardingKeyCount = this.requireShardingKeys.size();



        Number allScanPartitionTimeout = (Number) properties.getOrDefault("allScanPartitionTimeout", 5);
        long period = TimeUnit.SECONDS.toMillis(allScanPartitionTimeout.longValue());
        scheduledFuture = ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
            IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
            ioExecutor.executeBlocking((Handler<Promise<Void>>) promise -> {
                try {
                    List<Partition> partitions = fetchPartitions(Collections.emptyMap());
                    if (partitions.isEmpty()) {
                        scheduledFuture.cancel(true);
                    }
                    allScanPartitions = null;
                } finally {
                    promise.tryComplete();
                }
            });
        }, 0, period, TimeUnit.SECONDS);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    @SneakyThrows
    public List<Partition> calculate(Map<String, RangeVariable> values) {
        if (values == null || values.isEmpty()) {
            if (allScanPartitions == null) {
                allScanPartitions = fetchPartitions(Collections.emptyMap());
            }
            return new ArrayList<>(allScanPartitions);
        }
        return fetchPartitions(values);
    }

    @NotNull
    @SneakyThrows
    private synchronized List<Partition> fetchPartitions(Map<String, RangeVariable> values) {
        try {
            ShardingTableHandler tableHandler = getTable();
            String router_service_address = (String) properties.getOrDefault("routerServiceAddress", "http://localhost:9066/routerserivce");
            long timeout = Long.parseLong(properties.getOrDefault("fetchTimeout", 30L).toString());
            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            if (timeout > 0) {
                builder.connectTimeout(timeout, TimeUnit.MILLISECONDS);
            }
            if (timeout > 0) {
                builder.readTimeout(timeout, TimeUnit.MILLISECONDS);
            }
            OkHttpClient okHttpClient = builder.build();
            RequestBody body = new FormBody.Builder()
                    .add("schemaName", tableHandler.getSchemaName())
                    .add("tableName", tableHandler.getTableName())
                    .add("condition", JsonUtil.toJson(values))
                    .build();

            final Request request = new Request.Builder()
                    .url(router_service_address)
                    .post(body)
                    .build();
            Call call = okHttpClient.newCall(request);
            Response response = call.execute();
            ResponseBody responseBody = response.body();
            String s = new String(responseBody.bytes());
            List<Map<String, Object>> list = JsonUtil.from(s, List.class);
            return getPartitions(list);
        } catch (Throwable throwable) {
            LOGGER.error("", throwable);
            throw throwable;
        }
    }

    @NotNull
    private List<Partition> getPartitions(List<Map<String, Object>> list) {
        List<Partition> partitions = new ArrayList<>();
        for (Map<String, Object> stringStringMap : list) {

            String targetName = (String) stringStringMap.get("targetName");

            String schema = (String) stringStringMap.get("schema");


            String table = (String) stringStringMap.get("table");

            Integer dbIndex = (Integer) stringStringMap.get("dbIndex");

            Integer tableIndex = (Integer) stringStringMap.get("dbIndex");

            Integer index = (Integer) stringStringMap.get("index");

            IndexDataNode indexDataNode = new IndexDataNode(targetName, schema, table, dbIndex, tableIndex, index);

            partitions.add(indexDataNode);
        }
        return partitions;
    }

    @Override
    public boolean isShardingDbKey(String name) {
        return this.shardingDbKeys.contains(name);
    }

    @Override
    public boolean isShardingTableKey(String name) {
        return this.shardingTableKeys.contains(name);
    }

    @Override
    public boolean isShardingTargetKey(String name) {
        return this.shardingTargetKeys.contains(name);
    }

    @Override
    public String getErUniqueID() {
        return this.erUniqueID;
    }

    @Override
    public ShardingTableType getShardingTableType() {
        return this.shardingTableType;
    }

    @Override
    public int requireShardingKeyCount() {
        return this.requireShardingKeys.size();
    }

    @Override
    public boolean requireShardingKeys(Set<String> shardingKeys) {
        return shardingKeys.containsAll(this.requireShardingKeys);
    }
}
