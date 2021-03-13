package io.mycat.replica;

import io.mycat.MetaClusterCurrent;
import io.mycat.ReplicaReporter;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.TimerConfig;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import org.apache.groovy.util.Maps;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.mycat.assemble.MycatTest.DB1;
import static io.mycat.assemble.MycatTest.DB2;

public class GaleraTest extends ReplicaTest {

    @Test
    public void test() {
        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setTimer(timerConfig);
        clusterConfig.setClusterType(ReplicaType.GARELA_CLUSTER.name());
        HashMap<String, DatasourceConfig> map = new HashMap<>();
        map.put("dsw1", CreateDataSourceHint.createConfig("dsw1", DB1));
        map.put("dsw2", CreateDataSourceHint.createConfig("dsw2", DB2));
        map.put("dsr1", CreateDataSourceHint.createConfig("dsr1", DB2));
        LinkedList<Runnable> runnables = new LinkedList<>();
        ReplicaSelectorManager manager = ReplicaSelectorRuntime.create(
                Arrays.asList(clusterConfig),
                map,
                new LoadBalanceManager(),
                name -> 0,
                (command, initialDelay, period, unit) -> {
                    runnables.add(command);
                    return () -> {

                    };
                }
        );
        manager.putHeartFlow("c0", "dsw1", checkGalera());
        manager.putHeartFlow("c0", "dsw2", checkGalera());
        manager.putHeartFlow("c0", "dsr1", checkGalera());
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        for (PhysicsInstance physicsInstance : manager.getPhysicsInstances()) {
            Assert.assertTrue(physicsInstance.asSelectRead());
        }
        Set<String> balanceTargets = new HashSet<>();
        test(() -> {
            balanceTargets.add(manager.getDatasourceNameByReplicaName("c0", false, null));
            return balanceTargets.size() == 3;
        });
        Assert.assertEquals(3, balanceTargets.size());

        Set<String> masterTargets = new HashSet<>();
        test(() -> {
            masterTargets.add(manager.getDatasourceNameByReplicaName("c0", true, null));
            return false;
        });
        Assert.assertEquals(2, masterTargets.size());
    }

    @Test
    public void testMasterBroken() {

        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setTimer(timerConfig);
        clusterConfig.setClusterType(ReplicaType.GARELA_CLUSTER.name());
        HashMap<String, DatasourceConfig> map = new HashMap<>();
        map.put("dsw1", CreateDataSourceHint.createConfig("dsw1", DB1));
        map.put("dsw2", CreateDataSourceHint.createConfig("dsw2", DB2));
        map.put("dsr1", CreateDataSourceHint.createConfig("dsr1", DB2));
        LinkedList<Runnable> runnables = new LinkedList<>();
        ReplicaSelectorManager manager = ReplicaSelectorRuntime.create(
                Arrays.asList(clusterConfig),
                map,
                new LoadBalanceManager(),
                name -> 0,
                (command, initialDelay, period, unit) -> {
                    runnables.add(command);
                    return () -> {

                    };
                }
        );
        manager.putHeartFlow("c0", "dsw1", makeBroken());
        manager.putHeartFlow("c0", "dsw2", checkGalera());
        manager.putHeartFlow("c0", "dsr1", checkGalera());


        //模拟第一主节点无法连接

        MetaClusterCurrent.register(Maps.of(ReplicaReporter.class, new ReplicaReporter() {
            @Override
            public void reportReplica(Map<String, List<String>> state) {
                List<String> c0 = state.get("c0");
                Assert.assertEquals("[dsw2]", c0.toString());
            }
        }));

        //三次错误后触发主从切换
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        {
            checkALlRight(manager);

        }
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        {
            checkALlRight(manager);

        }
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        {
            PhysicsInstance dsw1 = manager.getPhysicsInstanceByName("dsw1");
            Assert.assertFalse(dsw1.asSelectRead());
            Assert.assertFalse(dsw1.isAlive());

            PhysicsInstance dsw2 = manager.getPhysicsInstanceByName("dsw2");
            Assert.assertTrue(dsw2.asSelectRead());
            Assert.assertTrue(dsw2.isAlive());

            PhysicsInstance dsr1 = manager.getPhysicsInstanceByName("dsr1");
            Assert.assertTrue(dsr1.asSelectRead());
            Assert.assertTrue(dsr1.isAlive());

        }

        //测试已经切换
        Set<String> balanceTargets = new HashSet<>();
        test(() -> {
            balanceTargets.add(manager.getDatasourceNameByReplicaName("c0", false, null));
            return false;
        });
        Assert.assertTrue(balanceTargets.size() <= 2);
        Assert.assertFalse(balanceTargets.contains("dsw1"));

        Set<String> masterTargets = new HashSet<>();
        test(() -> {
            masterTargets.add(manager.getDatasourceNameByReplicaName("c0", true, null));
            return false;
        });
        Assert.assertEquals(1, masterTargets.size());
        Assert.assertTrue(masterTargets.contains("dsw2"));

        manager.putHeartFlow("c0", "dsw2", checkGalera());
        manager.putHeartFlow("c0", "dsw1", checkGalera());
        manager.putHeartFlow("c0", "dsr1", checkGalera());
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        checkALlRight(manager);

        //切换后又发生短暂的连接损坏,因为新建心跳对象导致切换周期重置,这里不测试最小切换周期
        manager.putHeartFlow("c0", "dsw2", makeBroken());
        manager.putHeartFlow("c0", "dsw1", checkGalera());
        manager.putHeartFlow("c0", "dsr1", checkGalera());

        MetaClusterCurrent.register(Maps.of(ReplicaReporter.class, new ReplicaReporter() {
            @Override
            public void reportReplica(Map<String, List<String>> state) {
                List<String> c0 = state.get("c0");
                Assert.assertEquals("[dsw1]", c0.toString());
            }
        }));
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        {
            PhysicsInstance dsw1 = manager.getPhysicsInstanceByName("dsw1");
            Assert.assertTrue(dsw1.asSelectRead());
            Assert.assertTrue(dsw1.isAlive());

            PhysicsInstance dsw2 = manager.getPhysicsInstanceByName("dsw2");
            Assert.assertFalse(dsw2.asSelectRead());
            Assert.assertFalse(dsw2.isAlive());

            PhysicsInstance dsr1 = manager.getPhysicsInstanceByName("dsr1");
            Assert.assertTrue(dsr1.asSelectRead());
            Assert.assertTrue(dsr1.isAlive());

        }
        //测试最小切换周期
        Consumer<HeartBeatStrategy> consumer = new Consumer<HeartBeatStrategy>() {
            int count = 0;

            @Override
            public void accept(HeartBeatStrategy heartBeatStrategy) {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    makeBroken().accept(heartBeatStrategy);
                } else {
                    checkGalera().accept(heartBeatStrategy);
                }
                count++;
            }
        };
        manager.putHeartFlow("c0", "dsw2", consumer);
        manager.putHeartFlow("c0", "dsw1", consumer);
        manager.putHeartFlow("c0", "dsr1", consumer);

        AtomicInteger switchCounter = new AtomicInteger();

        MetaClusterCurrent.register(Maps.of(ReplicaReporter.class, new ReplicaReporter() {
            @Override
            public void reportReplica(Map<String, List<String>> state) {
                switchCounter.getAndIncrement();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
            }
        }));
        for (int i = 0; i < 1000; i++) {
            for (Runnable runnable : runnables) {
                runnable.run();
            }
        }
        Assert.assertTrue(switchCounter.get() < 100);
    }

    public static Consumer<HeartBeatStrategy> checkGalera(){
       return checkGalera(0);
    }
    @NotNull
    public static Consumer<HeartBeatStrategy> checkGalera(double delay) {
        return heartBeatStrategy -> {
            Assert.assertEquals("show status like 'wsrep%'", heartBeatStrategy.getSql());

            List<Map<String, Object>> list = new ArrayList<>();
            {
                Map<String, Object> map = new HashMap<>();
                map.put("Variable_name", "wsrep_cluster_status");
                map.put("Value", "Primary");
                list.add(map);
            }
            {
                Map<String, Object> map = new HashMap<>();
                map.put("Variable_name", "wsrep_connected");
                map.put("Value", "ON");
                list.add(map);
            }
            {
                Map<String, Object> map = new HashMap<>();
                map.put("Variable_name", "wsrep_ready");
                map.put("Value", "ON");
                list.add(map);
            }
            {
                Map<String, Object> map = new HashMap<>();
                map.put("Variable_name", "wsrep_flow_control_paused");
                map.put("Value", delay+"");
                list.add(map);
            }
            heartBeatStrategy.process(list);
            return;
        };
    }

    @Test
    public void testSlaveBroken() {
        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setClusterType(ReplicaType.GARELA_CLUSTER.name());
        clusterConfig.setTimer(timerConfig);
        HashMap<String, DatasourceConfig> map = new HashMap<>();
        map.put("dsw1", CreateDataSourceHint.createConfig("dsw1", DB1));
        map.put("dsw2", CreateDataSourceHint.createConfig("dsw2", DB2));
        map.put("dsr1", CreateDataSourceHint.createConfig("dsr1", DB2));
        LinkedList<Runnable> runnables = new LinkedList<>();
        ReplicaSelectorManager manager = ReplicaSelectorRuntime.create(
                Arrays.asList(clusterConfig),
                map,
                new LoadBalanceManager(),
                name -> 0,
                (command, initialDelay, period, unit) -> {
                    runnables.add(command);
                    return () -> {

                    };
                }
        );
        MetaClusterCurrent.register(Maps.of(ReplicaReporter.class, new ReplicaReporter() {
            @Override
            public void reportReplica(Map<String, List<String>> state) {
                List<String> c0 = state.get("c0");
                Assert.assertEquals("[dsw2]", c0.toString());
            }
        }));
        manager.putHeartFlow("c0", "dsw1", checkGalera());
        manager.putHeartFlow("c0", "dsw2", checkGalera());
        manager.putHeartFlow("c0", "dsr1", makeBroken());

        for (Runnable runnable : runnables) {
            runnable.run();
        }

        for (Runnable runnable : runnables) {
            runnable.run();
        }

        for (Runnable runnable : runnables) {
            runnable.run();
        }

        //测试已经切换
        Set<String> balanceTargets = new HashSet<>();
        test(() -> {
            balanceTargets.add(manager.getDatasourceNameByReplicaName("c0", false, null));
            return false;
        });
        Assert.assertFalse(balanceTargets.contains("dsr1"));

        Set<String> masterTargets = new HashSet<>();
        test(() -> {
            masterTargets.add(manager.getDatasourceNameByReplicaName("c0", true, null));
            return false;
        });
        Assert.assertEquals(2, masterTargets.size());
        Assert.assertTrue(masterTargets.contains("dsw1"));

        manager.putHeartFlow("c0", "dsr1", checkGalera());

        for (Runnable runnable : runnables) {
            runnable.run();
        }


        PhysicsInstance dsr1 = manager.getPhysicsInstanceByName("dsr1");
        Assert.assertTrue(dsr1.asSelectRead());
        Assert.assertTrue(dsr1.isAlive());
    }

    @Test
    public void testSlaveDelay() {
        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setClusterType(ReplicaType.GARELA_CLUSTER.name());
        clusterConfig.setTimer(timerConfig);
        HashMap<String, DatasourceConfig> map = new HashMap<>();
        map.put("dsw1", CreateDataSourceHint.createConfig("dsw1", DB1));
        map.put("dsw2", CreateDataSourceHint.createConfig("dsw2", DB2));
        map.put("dsr1", CreateDataSourceHint.createConfig("dsr1", DB2));
        LinkedList<Runnable> runnables = new LinkedList<>();
        ReplicaSelectorManager manager = ReplicaSelectorRuntime.create(
                Arrays.asList(clusterConfig),
                map,
                new LoadBalanceManager(),
                name -> 0,
                (command, initialDelay, period, unit) -> {
                    runnables.add(command);
                    return () -> {

                    };
                }
        );
        MetaClusterCurrent.register(Maps.of(ReplicaReporter.class, new ReplicaReporter() {
            @Override
            public void reportReplica(Map<String, List<String>> state) {
                List<String> c0 = state.get("c0");
                Assert.assertEquals("[dsw2]", c0.toString());
            }
        }));
        manager.putHeartFlow("c0", "dsw1", checkGalera());
        manager.putHeartFlow("c0", "dsw2", checkGalera());
        manager.putHeartFlow("c0", "dsr1",checkGalera(Long.MAX_VALUE));

        for (Runnable runnable : runnables) {
            runnable.run();
        }

        {
            Set<String> balanceTargets = new HashSet<>();
            test(() -> {
                balanceTargets.add(manager.getDatasourceNameByReplicaName("c0", false, null));
                return false;
            });
            Assert.assertTrue(balanceTargets.size() <= 2);
            Assert.assertFalse(balanceTargets.contains("dsr1"));
        }


        //延迟恢复
        {
            manager.putHeartFlow("c0", "dsr1", checkGalera());
            for (Runnable runnable : runnables) {
                runnable.run();
            }

            Set<String>    balanceTargets = new HashSet<>();
            test(() -> {
                balanceTargets.add(manager.getDatasourceNameByReplicaName("c0", false, null));
                return false;
            });
            Assert.assertTrue(balanceTargets.contains("dsr1"));
        }





    }

}
