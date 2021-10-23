package io.mycat.sqlhandler.config;

import io.mycat.config.KVObject;
import io.mycat.util.JsonUtil;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ZookeeperKVImpl<T extends KVObject> implements KV<T> {
    final ZooMap zooMap;
    final Class aClass;

    public ZookeeperKVImpl(ZooMap zooMap, Class aClass) {
//        String zk_address = System.getProperty("zk_address");
//        if (zk_address == null) {
//            if (MetaClusterCurrent.exist(MycatServerConfig.class)) {
//                MycatServerConfig serverConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class);
//                zk_address = (String) serverConfig.getProperties().get("zk_address");
//            }
//        }
//        Optional<ZooMap> o = Optional.ofNullable(zk_address).map(zkAddress -> {
//            ZKBuilder zkBuilder = new ZKBuilder(zkAddress);
//            return ZooMap.newMap(zkBuilder.build(), "/mycat");
//        });
//        this.zooMap = o.orElse(null);
        this.zooMap = zooMap;
        this.aClass = aClass;

    }

    @Override
    public Optional<T> get(String key) {
        return Optional.ofNullable(this.zooMap.get(key)).map(s -> (T) JsonUtil.from(s, aClass));
    }

    @Override
    public void removeKey(String key) {
        this.zooMap.remove(key);
    }

    @Override
    public void put(String key, T value) {
        this.zooMap.put(key, JsonUtil.toJson(value));
    }

    @Override
    public List<T> values() {
        Collection<String> values = this.zooMap.values();
        List<T> collect = (List) values.stream().map(i -> JsonUtil.from(i, aClass)).collect(Collectors.toList());
        return collect;
    }
}
