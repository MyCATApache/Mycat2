//package io.mycat.plug;
//
//import io.mycat.Hint;
//import io.mycat.MycatConfig;
//import io.mycat.config.PlugRootConfig;
//import io.mycat.plug.command.MycatCommandLoader;
//import io.mycat.plug.hint.HintLoader;
//import io.mycat.plug.loadBalance.LoadBalanceStrategy;
//import io.mycat.plug.sequence.SequenceGenerator;
//import org.junit.Assert;
//import org.junit.Test;
//
//public class PlugRuntimeTest {
//    MycatConfig defaultConfig = new MycatConfig();
//
//    @Test
//    public void update() {
//        PlugRuntime.INSTANCE.load(defaultConfig);
//        String name =  "testBL";
//        LoadBalanceStrategy loadBalanceByBalanceName = PlugRuntime.INSTANCE.getLoadBalanceByBalanceName(name);
//        ///////////////////////////////////////////////////////////////////////////////////////////////////
//        Assert.assertFalse(loadBalanceByBalanceName.getClass().getName().contains("BalanceRunOnMaster"));
//        ///////////////////////////////////////////////////////////////////////////////////////////////////
//        MycatConfig mycatConfig = new MycatConfig();
//
//        //添加插件
//        PlugRootConfig.LoadBalanceConfig loadBalanceConfig = new PlugRootConfig.LoadBalanceConfig();
//        loadBalanceConfig.setClazz("io.mycat.plug.loadBalance.BalanceRunOnMaster");
//        loadBalanceConfig.setName(name);
//        mycatConfig.getPlug().getLoadBalance().getLoadBalances().add(loadBalanceConfig);
//
//        //添加命令
//        PlugRootConfig.MycatCommandConfig mycatCommandConfig = new PlugRootConfig.MycatCommandConfig();
//        mycatCommandConfig.setName("testCmd");
//        mycatCommandConfig.setClazz(DemoEnum.class.getName());
//        mycatConfig.getPlug().getCommand().getCommands().add(mycatCommandConfig);
//
//        //添加序列
//        PlugRootConfig.SequenceConfig sequenceConfig = new PlugRootConfig.SequenceConfig();
//        sequenceConfig.setArgs("workerId:1");
//        sequenceConfig.setClazz("io.mycat.plug.sequence.SequenceSnowflakeGenerator");
//        sequenceConfig.setName("testSeq");
//        mycatConfig.getPlug().getSequence().getSequences().add(sequenceConfig);
//
//        PlugRootConfig.HintConfig hintConfig = new PlugRootConfig.HintConfig();
//        hintConfig.setArgs("demoHint");
//        hintConfig.setClazz(DemoHint.class.getName());
//        hintConfig.setName("demoHint");
//        mycatConfig.getPlug().getHint().getHints().add(hintConfig);
//
//        PlugRuntime.INSTANCE.load(mycatConfig);
//        LoadBalanceStrategy loadBalanceByBalanceName1 = PlugRuntime.INSTANCE.getLoadBalanceByBalanceName(name);
//        ///////////////////////////////////////////////////////////////////////////////////////////////////
//        Assert.assertTrue(loadBalanceByBalanceName1.getClass().getName().contains("BalanceRunOnMaster"));
//        ///////////////////////////////////////////////////////////////////////////////////////////////////
//
//        Assert.assertTrue(MycatCommandLoader.INSTANCE.get("testCmd").getClass().getName().contains("Demo"));
//        Assert.assertTrue(SequenceGenerator.INSTANCE.getSequence("testSeq").getClass().getName().contains("Snowflake"));
//        Hint demoHint = HintLoader.INSTANCE.get("demoHint");
//        Assert.assertTrue(demoHint.getName().equalsIgnoreCase("demoHint"));
//    }
//
//}