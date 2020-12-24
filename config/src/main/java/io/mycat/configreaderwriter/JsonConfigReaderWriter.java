package io.mycat.configreaderwriter;

import io.mycat.ConfigReaderWriter;
import io.mycat.util.JsonUtil;

import java.util.Arrays;
import java.util.Collection;

public class JsonConfigReaderWriter implements ConfigReaderWriter {
    @Override
    public Collection<String> getSuffixSet() {
        return Arrays.asList("json");
    }

    @Override
    public <T> T transformation(String text, Class<T> clazz) {
        return JsonUtil.from(text, clazz);
    }

    @Override
    public String transformation(Object dump) {
        return JsonUtil.toJson(dump);
    }
}