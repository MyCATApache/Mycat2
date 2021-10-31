package io.mycat;

import io.mycat.config.MycatRouterConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MetadataStorageManager implements ConfigReporter {

    public abstract void start() throws Exception;
    public abstract void start(MycatRouterConfig mycatRouterConfig);
    public abstract void reportReplica(Map<String, List<String>> dsNames);

    public abstract ConfigOps startOps();

    public abstract MycatRouterConfig fetchFromStore();

    public abstract void sync(MycatRouterConfig mycatRouterConfig, State state);

    @EqualsAndHashCode
    @Data
    public static class State {
        final Map<String, List<String>> replica = new HashMap<>();
    }

}