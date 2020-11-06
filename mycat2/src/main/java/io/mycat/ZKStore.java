package io.mycat;

import lombok.SneakyThrows;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ZKStore implements CoordinatorMetadataStorageManager.Store {
    private final Map<String, CoordinatorMetadataStorageManager.ChangedCallback> changedCallbackMap
            = new ConcurrentHashMap<>();
    private final CuratorFramework client;
    private final String CONFIG_PREFIX;
    private volatile List<CuratorOp> transactionOps = null;

    // 初始化zk连接
    public ZKStore(String configPrefix,
                   String address) {

        if (!configPrefix.startsWith("/")) {
            configPrefix = "/" + configPrefix;
        }
        if (configPrefix.endsWith("/")) {
            configPrefix = configPrefix.substring(0, configPrefix.length() - 1);
        }
        this.CONFIG_PREFIX = configPrefix;
        this.client = CuratorFrameworkFactory.newClient(address, new RetryNTimes(3, 1000));
        this.client.start();
        this.init();
    }

    @Override
    public void addChangedCallback(CoordinatorMetadataStorageManager.ChangedCallback changedCallback) {
        changedCallbackMap.put(changedCallback.getInterestedPath(),changedCallback);
    }

    @Override
    public synchronized void begin() {
        if (this.transactionOps != null) {
            throw new UnsupportedOperationException();
        }
        this.transactionOps = new ArrayList<>();
    }

    @SneakyThrows
    public String get(String schema) {
        String point = String.join("/", CONFIG_PREFIX, schema);
        return new String(client.getData().forPath(point));
    }

    @SneakyThrows
    @Override
    public void set(String name, String value) {
        String configFullName = String.join("/", CONFIG_PREFIX, name);
        InterProcessMutex interProcessMutex = new InterProcessMutex(client, configFullName);
        try {
            interProcessMutex.acquire();
            ///////////////////////////////////////////////////////////////////////
            Stat stat = client.checkExists().forPath(configFullName);
            if (stat == null) {
                if (this.transactionOps != null) {
                    this.transactionOps.add(
                            this.client.transactionOp().create().withMode(CreateMode.PERSISTENT)
                                    .forPath(configFullName, value.getBytes())
                    );
                } else {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                            .forPath(configFullName, value.getBytes());
                }
            } else {
                if (this.transactionOps != null) {
                    this.transactionOps.add(
                            this.client.transactionOp().setData().forPath(configFullName, value.getBytes()
                            )
                    );
                } else {
                    client.setData().forPath(configFullName, value.getBytes());
                }
            }
            ///////////////////////////////////////////////////////////////////////
        } finally {
            if (interProcessMutex.isAcquiredInThisProcess()) {
                interProcessMutex.release();
            }
        }

    }

    @SneakyThrows
    @Override
    public void set(String name, Map<String, String> map) {
        String configFullName = String.join("/", CONFIG_PREFIX, name);
        InterProcessMutex interProcessMutex = new InterProcessMutex(client, configFullName);
        try {
            interProcessMutex.acquire();
            ///////////////////////////////////////////////////////////////////////
            for (Map.Entry<String, String> e : map.entrySet()) {
                String key = e.getKey();
                String value = e.getValue();
                String path = String.join(configFullName, key);

                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    if (this.transactionOps != null) {
                        transactionOps.add(
                                client.transactionOp()
                                        .create()
                                        .withMode(CreateMode.PERSISTENT).forPath(configFullName)

                        );
                        transactionOps.add(
                                client.transactionOp()
                                        .create()
                                        .withMode(CreateMode.PERSISTENT).forPath(path, value.getBytes())
                        );

                    } else {
                        client.create().creatingParentsIfNeeded()
                                .withMode(CreateMode.PERSISTENT).forPath(configFullName, value.getBytes());
                    }
                } else {
                    client.setData().forPath(configFullName, value.getBytes());
                }
            }
            ///////////////////////////////////////////////////////////////////////
        } finally {
            if (interProcessMutex.isAcquiredInThisProcess()) {
                interProcessMutex.release();
            }
        }
    }

    @SneakyThrows
    public Map<String, String> getMap(String name) {
        String point = String.join("/", CONFIG_PREFIX, name);
        List<String> childrenNames = client.getChildren().forPath(point);
        Map<String, String> res = new HashMap<>();
        for (String childrenName : childrenNames) {
            res.put(childrenName,
                    new String(
                            client.getData().forPath(String.join(CONFIG_PREFIX, name, childrenName))
                    ));
        }
        return res;
    }

    @Override
    @SneakyThrows
    public synchronized void commit() {
        try {
            if (!transactionOps.isEmpty()) {
                Collection<CuratorTransactionResult> results = client.transaction().forOperations(transactionOps);
            }
        } finally {
            transactionOps = null;
        }
    }

    @Override
    public void close() {
        transactionOps = null;
        client.close();
    }

    @SneakyThrows
    public void init() {
        PathChildrenCache watcher = new PathChildrenCache(client, this.CONFIG_PREFIX, true);
        watcher.getListenable().addListener((curatorFramework, event) -> {

            String path = event.getData().getPath();
            path = path.substring(this.CONFIG_PREFIX.length());
            if (path.contains("/")) {
                path = path.substring(1);
            }
            if (PathChildrenCacheEvent.Type.CHILD_ADDED.equals(event.getType())
                    ||
                    PathChildrenCacheEvent.Type.CHILD_UPDATED.equals(event.getType())) {
                CoordinatorMetadataStorageManager.ChangedCallback changedCallback = changedCallbackMap.get(path);
                if (changedCallback != null) {
                    changedCallback.onChanged(path, new String(event.getData().getData()), false);
                }
            } else if (PathChildrenCacheEvent.Type.CHILD_REMOVED.equals(event.getType())) {
                /////////////////////////////////////////////////////////////
                CoordinatorMetadataStorageManager.ChangedCallback changedCallback = changedCallbackMap.get(path);
                if (changedCallback != null) {
                    changedCallback.onChanged(path, new String(event.getData().getData()), true);
                }
                /////////////////////////////////////////////////////////////
            }
        });
        watcher.start();
    }
}