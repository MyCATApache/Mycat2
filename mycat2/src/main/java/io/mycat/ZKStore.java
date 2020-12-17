package io.mycat;

import io.mycat.util.NameMap;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class ZKStore implements CoordinatorMetadataStorageManager.Store {
    private final ZooMap root;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKStore.class);
    private final NameMap<Entry> map = new NameMap<>();

    @Data

    static class Entry {
        private final TreeCache nodeCache;
        ZooMap zk;
        CoordinatorMetadataStorageManager.ChangedValueCallback callback;

        public Entry(ZooMap zk) throws Exception {
            this.zk = zk;
            this.callback = null;

            String root = zk.getRoot();
            this.nodeCache = new TreeCache(ZooMap.getClient(), root);
            Listenable<TreeCacheListener> listenable1 = nodeCache.getListenable();
            listenable1.addListener(new TreeCacheListener() {
                @Override
                public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                    TreeCacheEvent.Type type = treeCacheEvent.getType();
                    switch (type) {
                        case NODE_ADDED:
                        case NODE_UPDATED:
                        case NODE_REMOVED:
                            break;
                        case CONNECTION_SUSPENDED:
                        case CONNECTION_RECONNECTED:
                        case CONNECTION_LOST:
                        case INITIALIZED:
                            return;
                    }
                    ChildData currentData = treeCacheEvent.getData();
                    String path = currentData.getPath();
                    String data = new String(treeCacheEvent.getData().getData());
                    if ("".equalsIgnoreCase(data)) {
                        return;
                    }
                    int end = path.lastIndexOf("/") + 1;
                    String name = path.substring(end);
                    path = path.substring(0, end);
                    if (callback != null) {
                        switch (treeCacheEvent.getType()) {
                            case NODE_ADDED:
                            case NODE_UPDATED:
                                callback.onPut(name, data);
                                break;
                            case NODE_REMOVED:
                                callback.onRemove(name);
                                break;
                            case CONNECTION_SUSPENDED:
                                break;
                            case CONNECTION_RECONNECTED:
                                break;
                            case CONNECTION_LOST:
                                break;
                            case INITIALIZED:
                                break;
                        }
                    }

                    LOGGER.debug("path: " + path + " data:{}" + data);
                }
            });
        }

        public void start() throws Exception {
            nodeCache.start();
        }
    }


    // 初始化zk连接
    public ZKStore(
            String address) throws Exception {
        ZooMap.connectionString = address;
        this.root = ZooMap.newMap("/mycat");

        map.put("schemas", new Entry(ZooMap.newMap("/mycat/schemas")));
        map.put("datasources", new Entry(ZooMap.newMap("/mycat/datasources")));
        map.put("clusters", new Entry(ZooMap.newMap("/mycat/clusters")));
        map.put("users", new Entry(ZooMap.newMap("/mycat/users")));
        map.put("sequences", new Entry(ZooMap.newMap("/mycat/sequences")));
        map.put("sqlcaches", new Entry(ZooMap.newMap("/mycat/sqlcaches")));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (root != null) {
                    root.close();
                }
            }
        }));
        for (Entry entry : map.values()) {
            entry.start();
        }
        this.init();


    }

    @Override
    public void addChangedCallback(CoordinatorMetadataStorageManager.ChangedValueCallback changedCallback) {
        Entry entry = this.map.get(changedCallback.getKey());
        if (entry != null) {
            entry.callback = changedCallback;
        } else {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public synchronized void begin() {

    }

    @SneakyThrows
    public String get(String schema) {
        return this.root.get(schema);
    }

    @SneakyThrows
    @Override
    public void set(String name, String value) {
        String s = this.root.get(name);
        if (!Objects.equals(s, (value))) {
            this.root.put(name, value);
        }

    }

    @SneakyThrows
    @Override
    public void set(String name, Map<String, String> map) {
        Entry entry = this.map.get(name, false);
        if (entry != null) {
            for (Map.Entry<String, String> e : map.entrySet()) {
                ZooMap zk = entry.getZk();
                if (!Objects.equals(zk.get(e.getKey()), (e.getValue()))) {
                    zk.put(e.getKey(), e.getValue());
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }

    }

    @SneakyThrows
    public Map<String, String> getMap(String name) {
        Entry entry = map.get(name, false);
        if (entry != null) {
            return Collections.unmodifiableMap(entry.getZk());
        }
        throw new UnsupportedOperationException();
    }

    @Override
    @SneakyThrows
    public synchronized void commit() {

    }

    @Override
    public void close() {

    }

    @SneakyThrows
    public void init() throws Exception {

    }
}