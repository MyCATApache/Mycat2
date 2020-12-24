package io.mycat.sqlhandler;

import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataStorageManager;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.ConfigOps;
import io.mycat.config.MycatRouterConfig;
import org.jetbrains.annotations.NotNull;

public class ConfigUpdater {
   public static MycatRouterConfigOps getOps(){
        MetadataStorageManager metadataStorageManager = MetaClusterCurrent.wrapper(MetadataStorageManager.class);
       return getOps(metadataStorageManager);
   }

    @NotNull
    public static MycatRouterConfigOps getOps(MetadataStorageManager metadataStorageManager) {
        try(ConfigOps configOps = metadataStorageManager.startOps()){
            MycatRouterConfig routerConfig = (MycatRouterConfig) configOps.currentConfig();
            MycatRouterConfigOps mycatRouterConfigOps = new MycatRouterConfigOps(routerConfig, configOps);
            return mycatRouterConfigOps;
        }
    }

}
