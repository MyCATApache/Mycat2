package io.mycat;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class MetadataStorageManager implements ReplicaReporter{

    public abstract void start() throws Exception;

    public abstract void reportReplica(Map<String, List<String>> dsNames);

    public abstract ConfigOps startOps();
}