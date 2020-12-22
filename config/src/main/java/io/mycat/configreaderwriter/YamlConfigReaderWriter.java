package io.mycat.configreaderwriter;

import io.mycat.ConfigReaderWriter;
import io.mycat.util.YamlUtil;
import lombok.SneakyThrows;

import java.util.Arrays;
import java.util.Collection;

public class YamlConfigReaderWriter implements ConfigReaderWriter {

    @Override
    public Collection<String> getSuffixSet() {
        return Arrays.asList("yml", "yaml");
    }

    @Override
    @SneakyThrows
    public <T> T transformation(String text, Class<T> clazz) {
        return YamlUtil.load(text, clazz);
    }

    @Override
    public String transformation(Object dump) {
        return YamlUtil.dump(dump);
    }

}