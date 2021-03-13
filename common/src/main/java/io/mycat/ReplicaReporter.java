package io.mycat;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ReplicaReporter {
   void reportReplica(Map<String, List<String>> state);
}
