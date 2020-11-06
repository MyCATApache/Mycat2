package io.mycat;

import io.mycat.configreaderwriter.JsonConfigReaderWriter;
import io.mycat.configreaderwriter.YamlConfigReaderWriter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public interface ConfigReaderWriter {

    Collection<String> getSuffixSet();

    <T> T transformation(String text, Class<T> clazz);

    String transformation(Object dump);

    static ConfigReaderWriter getReaderWriterBySuffix(String suffix) {
        List<ConfigReaderWriter> configReaderWriters = Arrays.asList(
                new JsonConfigReaderWriter(),
                new YamlConfigReaderWriter()
        );
        for (ConfigReaderWriter configReaderWriter : configReaderWriters) {
            if (configReaderWriter.getSuffixSet().contains(suffix)) {
                return configReaderWriter;
            }
        }
        throw new IllegalArgumentException("can not find " + suffix + " reader writer");
    }

    static <T> T transformation(String suffix, String text, Class<T> clazz) {
        return getReaderWriterBySuffix(suffix).transformation(text, clazz);
    }
}