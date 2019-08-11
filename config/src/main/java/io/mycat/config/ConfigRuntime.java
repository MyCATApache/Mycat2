package io.mycat.config;

import io.mycat.config.datasource.MasterIndexesRootConfig;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public enum ConfigRuntime {
  INSTCANE;
  volatile String resourcesPath;
  volatile MasterIndexesRootConfig masterIndexesRootConfig;

  ConfigRuntime() {
    String configResourceKeyName = "MYCAT_HOME";
    String resourcesPath = System.getProperty(configResourceKeyName);
    if (resourcesPath == null) {
      resourcesPath = Paths.get("").toAbsolutePath().toString();
    }
    log("config folder path:{}", resourcesPath);
    log(configResourceKeyName, resourcesPath);
    if (resourcesPath == null || Boolean.getBoolean("DEBUG")) {
      try {
        resourcesPath = getResourcesPath(Class.forName("io.mycat.MycatCore"));
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    this.resourcesPath = resourcesPath;
    init(resourcesPath);
  }

  private static void log(String tmp, Object... args) {
    System.out.println(MessageFormat.format(tmp, args));
  }

  public static String getResourcesPath(Class clazz) {
    try {
      return Paths.get(
          Objects.requireNonNull(clazz.getProtectionDomain().getCodeSource().getLocation().toURI()))
          .toAbsolutePath()
          .toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void init(String resourcesPath) {
    try {
      ConfigReceiver receiver = ConfigLoader.load(resourcesPath, GlobalConfig.genVersion());
      MasterIndexesRootConfig masterIndexesRootConfig = receiver
          .getConfig(ConfigEnum.REPLICA_INDEX);
      Objects.requireNonNull(masterIndexesRootConfig);
      this.masterIndexesRootConfig = masterIndexesRootConfig;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void reload(String path) {
    Objects.requireNonNull(path);
    String argPath = Paths.get(path).toAbsolutePath().toString();
    if (argPath.equalsIgnoreCase(resourcesPath)) {
      this.resourcesPath = argPath;
      init(this.resourcesPath);
    }

  }

  public String getResourcesPath() {
    return resourcesPath;
  }

  public Set<Integer> getReplicaIndexes(String replicaName) {
    Objects.requireNonNull(masterIndexesRootConfig);
    return ReplicaIndexesModifier
        .getReplicaIndexes(masterIndexesRootConfig.getMasterIndexes(), replicaName);
  }

  public void modifyReplicaMasterIndexes(final String replicaName,
      final List<Integer> oldWriteDataSource, final List<Integer> newWriteDataSource) {
    Objects.requireNonNull(masterIndexesRootConfig);
    ReplicaIndexesModifier
        .updateReplicaMasterIndexesConfig(masterIndexesRootConfig, replicaName, oldWriteDataSource,
            newWriteDataSource);
  }


}