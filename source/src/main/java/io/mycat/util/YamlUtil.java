package io.mycat.util;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Desc: yml文件的工具类
 *
 * @date: 09/09/2017
 * @author: gaozhiwen
 */
public class YamlUtil {
    private static final String ROOT_PATH = YamlUtil.class.getClassLoader().getResource("").getPath();

    public static <T> T load(String fileName, Class<T> clazz) throws FileNotFoundException {
        FileInputStream fis = null;
        try {
            URL url = YamlUtil.class.getClassLoader().getResource(fileName);
            if (url != null) {
                Yaml yaml = new Yaml();
                fis = new FileInputStream(url.getFile());
                T obj = yaml.loadAs(fis, clazz);
                return obj;
            }
            return null;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    public static String dump(Object obj) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        String str = yaml.dump(obj);
        return str;
    }

    public static void dumpToFile(String path, String content) throws IOException {
        String dirPath = createDirectoryIfNotExists("prepare/");
        Path file = Paths.get(dirPath + path);
        try (FileWriter writer = new FileWriter(file.toString())) {
            writer.write(content);
        }
    }

    /**
     * 将配置文件归档，新的配置文件生效
     */
    public static void archive(String configName, int curVersion) throws IOException {
        //检查是否有新需要生成的文件
        String preparePath = createDirectoryIfNotExists("prepare/");
        File prepareFile = new File(preparePath);
        File[] files = prepareFile.listFiles((dir, name) -> name.startsWith(configName));
        if (files == null || files.length == 0) {
            return;
        }
        //将旧的配置归档
        String archivePath = createDirectoryIfNotExists("archive/");
        Files.move(Paths.get(ROOT_PATH + configName), Paths.get(archivePath + configName + "-" + curVersion), StandardCopyOption.REPLACE_EXISTING);
        //将新的配置生效
        File confFile = Stream.of(files).sorted((file1, file2) -> {
                String name1 = file1.getName();
                Integer version1 = Integer.valueOf(name1.substring(name1.lastIndexOf("-") + 1));
                String name2 = file2.getName();
                Integer version2 = Integer.valueOf(name2.substring(name2.lastIndexOf("-") + 1));
                return version2.compareTo(version1);
            }).findFirst().orElse(null);
        Files.copy(confFile.toPath(), Paths.get(ROOT_PATH + configName), StandardCopyOption.REPLACE_EXISTING);
        //删除配置
        Files.delete(confFile.toPath());
    }

    /**
     * 创建指定的文件夹
     * @param directoryName
     * @return 返回创建的文件夹路径
     */
    private static String createDirectoryIfNotExists(String directoryName) throws IOException {
        String dirPath = ROOT_PATH + directoryName;
        Path directory = Paths.get(dirPath);
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
        return dirPath;
    }
}
