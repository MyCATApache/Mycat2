package io.mycat.replica;

import io.mycat.replica.heartbeat.DatasourceEnum;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import org.apache.groovy.util.Maps;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public abstract class ReplicaTest {

    public static void test(BooleanSupplier booleanSupplier) {
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
        while (true) {
            if (System.currentTimeMillis() > end || booleanSupplier.getAsBoolean()) {
                break;
            }
        }
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> checkSelect1() {
        return heartBeatStrategy -> {
            Assert.assertEquals("select 1", heartBeatStrategy.getSqls().get(0));
            Map<String, Object> map13 = new HashMap<>();
            map13.put("1", 1);
            List<Map<String, Object>> list = Arrays.asList(map13);
            heartBeatStrategy.process(Collections.singletonList(list));
        };
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> makeBroken() {
        return heartBeatStrategy -> {
            RuntimeException runtimeException = new RuntimeException("no access");
            heartBeatStrategy.onException(runtimeException);
        };
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> checkShowSlaveStatus() {
        return checkShowSlaveStatus(0);
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> checkShowSlaveStatus(int delay) {
        return heartBeatStrategy -> {
            Assert.assertEquals("show slave status", heartBeatStrategy.getSqls().get(0));
            Map<String, Object> map13 = new HashMap<>();
            map13.put("Slave_IO_Running", "Yes");
            map13.put("Slave_SQL_Running", "Yes");
            map13.put("Seconds_Behind_Master", delay +"");
            List<Map<String, Object>> list = Arrays.asList(map13);
            heartBeatStrategy.process(Collections.singletonList(list));
        };
    }

    @NotNull
    public static Consumer<HeartBeatStrategy> checkMasterSlave() {
        return heartBeatStrategy -> {
            switch (heartBeatStrategy.getSqls().get(0)) {
                case "select 1": {
                    checkSelect1().accept(heartBeatStrategy);
                    return;
                }
                case "show slave status": {
                    checkShowSlaveStatus().accept(heartBeatStrategy);
                    return;
                }
            }
        };
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


}
