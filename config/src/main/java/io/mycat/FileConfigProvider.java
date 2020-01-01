package io.mycat;

import io.mycat.util.YamlUtil;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;

public class FileConfigProvider implements ConfigProvider {
    volatile MycatConfig config;
    private String defaultPath;

    @Override
    public void init(Map<String, String> config) throws Exception {
        this.defaultPath = config.get("path");
        fetchConfig(this.defaultPath);
    }

    @Override
    public void fetchConfig() throws Exception {
        fetchConfig(defaultPath);
    }

    @Override
    public void report(Map<String, Object> changed) {

    }

    @Override
    public void fetchConfig(String url) throws Exception {
        Path asbPath = Paths.get(url).toAbsolutePath();
        if (!Files.exists(asbPath)) {
            throw new IllegalArgumentException(MessageFormat.format("path not found: {0}", Objects.toString(asbPath)));
        }
        config = YamlUtil.load(asbPath.toString(),MycatConfig.class);
    }


    @Override
    public MycatConfig currentConfig() {
        return config;
    }
}