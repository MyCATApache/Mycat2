package io.mycat.config;

import io.mycat.config.datasource.MasterIndexesRootConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaIndexesModifier {

  private static final Logger REPLICA_MASTER_INDEXES_LOGGER = LoggerFactory
      .getLogger("replicaIndexesLogger");
  private static final AtomicBoolean LOCK = new AtomicBoolean(false);

  public static Set<Integer> getReplicaIndexes(Map<String, String> replicaIndexes,
      String replicaName) {
    String writeIndexText = replicaIndexes.get(replicaName);
    Set<Integer> writeIndex;
    if (writeIndexText == null || "".equals(writeIndexText)) {
      writeIndex = Collections.singleton(0);
      REPLICA_MASTER_INDEXES_LOGGER.warn("master indexes is empty and set  master of {} is 0 index",
          replicaName);
    } else {
      if (writeIndexText.contains(",")) {
        List<String> strings = Arrays.asList(writeIndexText.split(","));
        writeIndex = strings.stream().map(Integer::parseInt).collect(Collectors.toSet());
      } else {
        writeIndex = Collections.singleton(Integer.parseInt(writeIndexText));
      }
    }
    return writeIndex;
  }


  public static void updateReplicaMasterIndexesConfig(MasterIndexesRootConfig config,
      final String replicaName,
      final List<Integer> oldWriteDataSource, final List<Integer> newWriteDataSource) {
    if (LOCK.compareAndSet(false, true)) {
      try {
        Map<String, String> masterIndexes = new HashMap<>(config.getMasterIndexes());
        String old = masterIndexes.get(replicaName);
        String switchRes = newWriteDataSource.stream().map(i -> String.valueOf(i))
            .collect(Collectors.joining(","));
        if (old.equalsIgnoreCase(switchRes)) {
          return;
        }
        String backup = YamlUtil.dump(config);
        YamlUtil.dumpBackupToFile(config.getFilePath(), config.getVersion(), backup);
        masterIndexes.put(replicaName, switchRes);
        config.setMasterIndexes(masterIndexes);
        config.setVersion(config.getVersion() + 1);
        String newContext = YamlUtil.dump(config);
        YamlUtil.dumpToFile(config.getFilePath(), newContext);
        REPLICA_MASTER_INDEXES_LOGGER.info("switchRes from:{}", old, switchRes);
      } finally {
        LOCK.set(false);
      }
    }
  }
}