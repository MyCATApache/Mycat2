package io.mycat.example.readWriteSeparation;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import lombok.SneakyThrows;

import java.nio.file.Paths;

public class ReadWriteSeparation {
    @SneakyThrows
    public static void main(String[] args) {
        String resource = Paths.get( ReadWriteSeparation.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ReadWriteSeparation.class);
        MycatCore.INSTANCE.init(bootConfig);
    }
}