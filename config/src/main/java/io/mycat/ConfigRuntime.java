package io.mycat;

import static jdk.nashorn.internal.objects.NativeMath.log;

import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.GlobalConfig;
import io.mycat.config.ReplicaIndexesModifier;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ConfigRuntime {
  INSTCANE;
  volatile String resourcesPath;
  volatile MasterIndexesRootConfig masterIndexesRootConfig;
  final Logger LOGGER = LoggerFactory.getLogger(ConfigRuntime.class);
  volatile ConfigReceiver lastConfig;

  ConfigRuntime() {
    String configResourceKeyName = "MYCAT_HOME";
    String resourcesPath = System.getProperty(configResourceKeyName);
    if (resourcesPath == null) {
      Path root = Paths.get("").toAbsolutePath();

      try {
        resourcesPath = Files
            .find(root, 5,
                (path, basicFileAttributes) -> {
                  if (Files.isDirectory(path)) {
                    return false;
                  }
                  return path.toString().endsWith("masterIndexes.yml");
                }).findFirst().map(i -> i.getParent()).orElse(root).toAbsolutePath()
            .toString();
      } catch (IOException e) {
        resourcesPath = root.toString();
      }
    }
    LOGGER.info("config folder path:{}", resourcesPath);
    log(configResourceKeyName, resourcesPath);
    if (resourcesPath == null || Boolean.getBoolean("DEBUG")) {
      try {
        resourcesPath = getResourcesPath(Class.forName("io.mycat.MycatCore"));
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    this.resourcesPath = resourcesPath;
    this.lastConfig = init(resourcesPath);
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

  private ConfigReceiver init(String resourcesPath) {
    try {
      ConfigReceiver receiver = ConfigLoader.load(resourcesPath, GlobalConfig.genVersion());
      MasterIndexesRootConfig masterIndexesRootConfig = receiver
          .getConfig(ConfigEnum.REPLICA_INDEX);
      Objects.requireNonNull(masterIndexesRootConfig);
      this.masterIndexesRootConfig = masterIndexesRootConfig;
      return receiver;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public ConfigReceiver load() {
    return lastConfig;
  }

  public ConfigReceiver load(String path) {
    Objects.requireNonNull(path);
    String argPath = Paths.get(path).toAbsolutePath().toString();
    if (!argPath.equalsIgnoreCase(resourcesPath)) {
      this.resourcesPath = argPath;
      return init(this.resourcesPath);
    }
    return lastConfig;
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