package io.mycat;

import java.util.Map;
import java.util.Set;

public abstract class MetadataStorageManager {

    public abstract void start() throws Exception;

    public abstract void reportReplica(Map<String, Set<String>> dsNames);

    public abstract ConfigOps startOps();
}