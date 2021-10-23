package io.mycat.sqlhandler.config;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.KVObject;
import io.mycat.config.MycatServerConfig;
import io.mycat.util.JsonUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ZookeeperManagerImpl extends AbstractStorageManagerImpl {
    Optional<ZooMap> zooMapOptional;
    @Override
    public void reportReplica(Map<String, List<String>> state) {
        Optional<ZooMap> zooMapOptional = getZooMapOptional();
        zooMapOptional.ifPresent(zooMap -> zooMap.put("replicaLog", JsonUtil.toJson(state)));
    }

    public Optional<ZooMap> getZooMapOptional() {
        if (zooMapOptional.isPresent()){
            return zooMapOptional;
        }
        String zk_address = System.getProperty("zk_address");
        if (zk_address == null) {
            if (MetaClusterCurrent.exist(MycatServerConfig.class)) {
                MycatServerConfig serverConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class);
                zk_address = (String) serverConfig.getProperties().get("zk_address");
            }
        }
        zooMapOptional = Optional.ofNullable(zk_address).map(zkAddress -> {
            ZKBuilder zkBuilder = new ZKBuilder(zkAddress);
            return ZooMap.newMap(zkBuilder.build(), "/mycat");
        });
        return zooMapOptional;
    }

    @Override
    public <T extends KVObject> KV<T> get(String path, String fileNameTemplate, Class<T> aClass) {
        Optional<ZooMap> zooMapOptional = getZooMapOptional();
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncFromNet() {

    }

    @Override
    public void syncToNet() {

    }

    @Override
    public boolean checkConfigConsistency() {
        return true;
    }
}
