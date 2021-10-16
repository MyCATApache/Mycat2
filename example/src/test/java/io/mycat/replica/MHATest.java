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
import io.mycat.replica.heartbeat.strategy.MGRHeartBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MHAHeartBeatStrategy;
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

public class MHATest extends ReplicaTest {


    @Test
    public void test() {
        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setTimer(timerConfig);
        clusterConfig.setClusterType(ReplicaType.MHA.name());
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
        manager.putHeartFlow("c0", "dsw1", checkMHA(true));
        manager.putHeartFlow("c0", "dsw2", checkMHA(false));
        manager.putHeartFlow("c0", "dsr1", checkMHA(false));
        manager.start();

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
        Assert.assertEquals(1, masterTargets.size());
    }

    @Test
    public void testSlaveBroken() {
        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setTimer(timerConfig);
        clusterConfig.setClusterType(ReplicaType.MHA.name());
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
        manager.putHeartFlow("c0", "dsw1", checkMHA(true));
        manager.putHeartFlow("c0", "dsw2", checkMHA(false));
        manager.putHeartFlow("c0", "dsr1", makeBroken());
        manager.start();

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
        Assert.assertEquals(1, masterTargets.size());
        Assert.assertTrue(masterTargets.contains("dsw1"));

        manager.putHeartFlow("c0", "dsr1", checkMHA(false));

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
        clusterConfig.setTimer(timerConfig);
        clusterConfig.setClusterType(ReplicaType.MHA.name());
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
        manager.putHeartFlow("c0", "dsw1", checkMHA(true));
        manager.putHeartFlow("c0", "dsw2", checkMHA(false));
        manager.putHeartFlow("c0", "dsr1", checkMHA(false, Integer.MAX_VALUE));
        manager.start();

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
            manager.putHeartFlow("c0", "dsr1", checkMHA(false, 0));
            for (Runnable runnable : runnables) {
                runnable.run();
            }

            Set<String> balanceTargets = new HashSet<>();
            test(() -> {
                balanceTargets.add(manager.getDatasourceNameByReplicaName("c0", false, null));
                return false;
            });
            Assert.assertTrue(balanceTargets.contains("dsr1"));
        }
    }


    @Test
    public void testMasterBroken() {

        ClusterConfig clusterConfig = CreateClusterHint.createConfig("c0", Arrays.asList("dsw1", "dsw2"), Arrays.asList("dsr1"));
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        timerConfig.setPeriod(1);
        timerConfig.setInitialDelay(0);
        clusterConfig.setTimer(timerConfig);
        clusterConfig.setClusterType(ReplicaType.MHA.name());
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
        manager.start();
        manager.putHeartFlow("c0", "dsw1", makeBroken());
        manager.putHeartFlow("c0", "dsw2", checkMHA(false, 0));
        manager.putHeartFlow("c0", "dsr1", checkMHA(false, 0));


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

        //触发切换
        manager.putHeartFlow("c0", "dsw2", checkMHA(true, 0));
        for (Runnable runnable : runnables) {
            runnable.run();
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

        manager.putHeartFlow("c0", "dsw2", checkMHA(true, 0));
        manager.putHeartFlow("c0", "dsw1", checkMHA(false, 0));
        manager.putHeartFlow("c0", "dsr1", checkMHA(false, 0));
        for (Runnable runnable : runnables) {
            runnable.run();
        }
        checkALlRight(manager);

        //切换后又发生短暂的连接损坏,因为新建心跳对象导致切换周期重置,这里不测试最小切换周期
        manager.putHeartFlow("c0", "dsw2", makeBroken());
        manager.putHeartFlow("c0", "dsw1", checkMHA(false, 0));
        manager.putHeartFlow("c0", "dsr1", checkMHA(false, 0));

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
            PhysicsInstance dsw1 = manager.getPhysicsInstanceByName("dsw2");
            Assert.assertFalse(dsw1.asSelectRead());
            Assert.assertFalse(dsw1.isAlive());

            PhysicsInstance dsw2 = manager.getPhysicsInstanceByName("dsw1");
            Assert.assertTrue(dsw2.asSelectRead());
            Assert.assertTrue(dsw2.isAlive());

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
                    checkMasterSlave().accept(heartBeatStrategy);
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

    public void checkALlRight(ReplicaSelectorManager manager) {
        PhysicsInstance dsw1 = manager.getPhysicsInstanceByName("dsw1");
        Assert.assertTrue(dsw1.asSelectRead());
        Assert.assertTrue(dsw1.isAlive());

        PhysicsInstance dsw2 = manager.getPhysicsInstanceByName("dsw2");
        Assert.assertTrue(dsw2.asSelectRead());
        Assert.assertTrue(dsw2.isAlive());

        PhysicsInstance dsr1 = manager.getPhysicsInstanceByName("dsr1");
        Assert.assertTrue(dsr1.asSelectRead());
        Assert.assertTrue(dsr1.isAlive());
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> checkMHA(boolean master) {
        return checkMHA(0, master);
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> checkMHA(boolean master, int delay) {
        return checkMHA(delay, master);
    }

    @NotNull
    private static Consumer<HeartBeatStrategy> checkMHA(int delay, boolean master) {
        return heartBeatStrategy -> {
            List<String> sqls = heartBeatStrategy.getSqls();
            List<List<Map<String, Object>>> list = new ArrayList<>();
            Assert.assertTrue(sqls.get(0).equalsIgnoreCase(MHAHeartBeatStrategy.READ_ONLY_SQL));
            Assert.assertTrue(sqls.get(1).equalsIgnoreCase(MHAHeartBeatStrategy.MASTER_SLAVE_HEARTBEAT_SQL));
            list.add(Arrays.asList(Maps.of("READ_ONLY", !master ? 1 : 0)));

            Map<String, Object> map13 = new HashMap<>();
            map13.put("Slave_IO_Running", "Yes");
            map13.put("Slave_SQL_Running", "Yes");
            map13.put("Seconds_Behind_Master", delay +"");

            list.add(Arrays.asList(map13));
            heartBeatStrategy.process(list);
        };
    }

}
