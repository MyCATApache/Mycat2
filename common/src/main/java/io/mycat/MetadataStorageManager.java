package io.mycat;

import java.util.Set;

public abstract class MetadataStorageManager {

    abstract void start() throws Exception ;

    public abstract void reportReplica(String name, Set<String> dsNames);

    public abstract ConfigOps startOps();


}