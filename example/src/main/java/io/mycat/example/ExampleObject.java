package io.mycat.example;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import io.mycat.example.booster.BoosterExample;
import io.mycat.example.customrule.CustomRuleExample;
import io.mycat.example.customrulesegmentquery.CustomRuleSegExample;
import io.mycat.example.manager.ManagerExample;
import io.mycat.example.pstmt.PstmtShardingExample;
import io.mycat.example.sharding.ShardingExample;
import io.mycat.example.shardinglocal.ShardingLocalExample;
import io.mycat.example.shardinglocalfail.ShardingLocalFailExample;
import io.mycat.example.shardingrw.ShardingRwExample;
import io.mycat.example.shardingxa.ShardingXAExample;
import io.mycat.example.shardingxafail.ShardingXAFailExample;
import io.mycat.util.NetUtil;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

public class ExampleObject {
    public static String resource = null;
    public static boolean test = false;
    public static String resultDir = null;
    public static boolean exit = false;
    public static boolean server = true;

    public static void main(String[] args, Class<?> currentClass) throws Exception {

        if (args != null && args.length > 0) {
            resource = args[0];
            if (args.length > 1) {
                resultDir = args[1];
            }
            List<String> strings = Arrays.asList(args);
            test = strings.contains("test");
            exit = strings.contains("exit");
            server = strings.contains("server");
        }
        if (resource == null) {
            resource = Paths.get(currentClass.getResource(".").toURI()).toAbsolutePath().toString();
        }
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        if (server && !NetUtil.isHostConnectable("0.0.0.0", 8066)) {
            ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(BoosterExample.class);
            MycatCore.INSTANCE.init(bootConfig);
        }
        if (test) {
            String message = "";
            try {
                Method method = currentClass.getMethod("test");
                method.invoke(null);
            } catch (Throwable e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                message =sw.toString();
            }
            System.out.println("-----------------------ok-------------------------");
            if (resultDir != null) {
                try {
                    Path resolve = Paths.get(resultDir).toAbsolutePath().resolve(currentClass.getSimpleName() + ".txt");
                    File file = resolve.toFile();
                    if (file.exists()) {
                        file.delete();
                    }
                    Files.write(resolve,
                            message.getBytes(),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            if (exit) {
                System.exit(0);
            }
        }
    }

    //生成测试文件
    public static void main(String[] args) throws Exception {
        List<Class> objects = Arrays.asList(
                BoosterExample.class,
                CustomRuleExample.class,
                CustomRuleSegExample.class,
                ManagerExample.class,
                PstmtShardingExample.class,
                io.mycat.example.readwriteseparation.ReadWriteSeparationExample.class,
                ShardingExample.class,
                ShardingLocalExample.class,
                ShardingLocalFailExample.class,
                ShardingRwExample.class,
                ShardingXAExample.class,
                ShardingXAFailExample.class
        );
        StringBuilder sb = new StringBuilder();
        for (Class object : objects) {
            String format = MessageFormat.format("java -cp target/lib/* {0} %curPath%/src/main/resources/{1} %curPath%/target test server exit\n",
                    object.getName(),object.getPackage().getName().replaceAll("\\.","/"));
            sb.append(format);
        }
        System.out.println(sb.toString());

    }
}