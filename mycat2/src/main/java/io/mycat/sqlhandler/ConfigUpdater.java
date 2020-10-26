package io.mycat.sqlhandler;

import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataStorageManager;
import io.mycat.MycatRouterConfigOps;
import io.mycat.ConfigOps;
import io.mycat.config.MycatRouterConfig;

public class ConfigUpdater {
   public static MycatRouterConfigOps getOps(){
        MetadataStorageManager metadataStorageManager = MetaClusterCurrent.wrapper(MetadataStorageManager.class);
        try(ConfigOps configOps = metadataStorageManager.startOps()){
            MycatRouterConfig routerConfig = (MycatRouterConfig) configOps.currentConfig();
            MycatRouterConfigOps mycatRouterConfigOps = new MycatRouterConfigOps(routerConfig, configOps);
            return mycatRouterConfigOps;
        }
    }

}
