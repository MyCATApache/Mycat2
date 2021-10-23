package io.mycat.sqlhandler.config;

import io.mycat.config.KVObject;
import io.mycat.util.JsonUtil;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FileKV<T extends KVObject> implements KV<T> {
    String fileNameTemplate;
    Class<T> aClass;
    Path dir;
    String suffix;

    public FileKV(String fileNameTemplate, Class<T> aClass, Path dir, String suffix) {
        this.fileNameTemplate = fileNameTemplate;
        this.aClass = aClass;
        this.dir = dir;
        this.suffix = suffix;
    }

    @Override
    public Optional< T> get(String key) {
        Path path1 = getPath(key);
        Optional<String> stringOptional = readString(path1);
        return stringOptional.map(s->{
            return JsonUtil.from(s, aClass);
        });
    }

    @Override
    @SneakyThrows
    public void removeKey(String key) {
        Files.deleteIfExists(getPath(key));
    }

    @NotNull
    private Path getPath(String key) {
        return dir.resolve(key + "." + fileNameTemplate + "." + suffix);
    }

    @Override
    public void put(String key, T value) {
        writeFile(JsonUtil.toJson(value), getPath(key));
    }

    @Override
    @SneakyThrows
    public List<T> values() {
        String s = "." + fileNameTemplate + "." + suffix;
        return Files.list(dir).filter(i -> i.toString().endsWith(s)).map(i -> {
            Optional<String> s1 = readString(i);
            return JsonUtil.from(s1.get(), aClass);
        }).collect(Collectors.toList());
    }

    @SneakyThrows
    public static void writeFile(String t, Path filePath) {
        FileUtils.write(filePath.toFile(), t);
    }

    @SneakyThrows
    Optional<String> readString(Path path) {
        if (!Files.exists(path)){
            return Optional.empty();
        }
        return Optional.ofNullable(new String(Files.readAllBytes(path)));
    }
}
