package io.mycat;

import java.util.List;
import java.util.Set;

public abstract class MetadataStorageManager {

    abstract void start() throws Exception;

    public abstract void reportReplica(String name, Set<String> dsNames);

    public abstract void addLearners(List<String> addressList);

    public abstract void removeLearners(List<String> addressList);

    public abstract String getLeaderAddress();

    public abstract boolean transferLeader(String address);

    public abstract ConfigOps startOps();
}