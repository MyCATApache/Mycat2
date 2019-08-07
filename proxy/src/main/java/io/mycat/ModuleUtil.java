package io.mycat;

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.YamlUtil;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.util.StringUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModuleUtil {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ModuleUtil.class);
  private static final Logger REPLICA_MASTER_INDEXES_LOGGER = LoggerFactory
      .getLogger("replicaIndexesLogger");

  public static Set<Integer> getReplicaIndexes(Map<String, String> replicaIndexes,
      ReplicaConfig replicaConfig) {
    String writeIndexText = replicaIndexes.get(replicaConfig.getName());
    Set<Integer> writeIndex;
    if (StringUtil.isEmpty(writeIndexText)) {
      writeIndex = Collections.singleton(0);
      LOGGER.warn("master indexes is empty and set  master of {} is 0 index",
          replicaConfig.getName());
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


  public static <T extends MycatDataSource> void updateReplicaMasterIndexesConfig(
      final MycatReplica replica,
      List<T> writeDataSource, final MasterIndexesRootConfig config) {

    synchronized (REPLICA_MASTER_INDEXES_LOGGER) {
      Map<String, String> masterIndexes = new HashMap<>(config.getMasterIndexes());
      String name = replica.getName();
      String old = masterIndexes.get(name);
      String switchRes = writeDataSource.stream().map(i -> String.valueOf(i.getIndex()))
          .collect(Collectors.joining(","));
      if (old.equalsIgnoreCase(switchRes)) {
        return;
      }
      String backup = YamlUtil.dump(config);
      YamlUtil.dumpBackupToFile(config.getFilePath(), config.getVersion(), backup);
      masterIndexes.put(name, switchRes);
      config.setMasterIndexes(masterIndexes);
      config.setVersion(config.getVersion() + 1);
      String newContext = YamlUtil.dump(config);
      YamlUtil.dumpToFile(config.getFilePath(), newContext);
      REPLICA_MASTER_INDEXES_LOGGER.info("switchRes from:{}", old, switchRes);
    }
  }
}