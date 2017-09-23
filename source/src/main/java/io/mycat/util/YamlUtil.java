package io.mycat.util;

import io.mycat.mycat2.ConfigLoader;
import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.proxy.Configurable;
import io.mycat.proxy.ProxyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(YamlUtil.class);
    private static String ROOT_PATH;

    static {
    	String mycatHome = System.getProperty("MYCAT_HOME");
        if (mycatHome == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(System.getProperty("user.dir"))
                .append(File.separator)
                .append("source")
                .append(File.separator)
                .append("target")
                .append(File.separator)
                .append("classes")
                .append(File.separator);
            ROOT_PATH = sb.toString();
            LOGGER.debug("MYCAT_HOME is not set, set the default path: {}", ROOT_PATH);
        } else {
            ROOT_PATH = mycatHome.endsWith(File.separator) ?
                    mycatHome + ConfigLoader.DIR_CONF :
                    mycatHome + File.separator + ConfigLoader.DIR_CONF;
            LOGGER.debug("mycat home: {}, root path: {}", mycatHome, ROOT_PATH);
        }
    }

    public static <T> T load(String fileName, Class<T> clazz) throws FileNotFoundException {
        InputStreamReader fis = null;
        try {
            URL url = YamlUtil.class.getClassLoader().getResource(fileName);
            if (url != null) {
                Yaml yaml = new Yaml();
                fis =new InputStreamReader(new FileInputStream(url.getFile()), StandardCharsets.UTF_8);
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
        Representer representer = new Representer();
        representer.addClassTag(obj.getClass(), Tag.MAP);
        Yaml yaml = new Yaml(representer, options);
        String str = yaml.dump(obj);
        return str;
    }

    public static void dumpToFile(String path, String content) {
        ProxyRuntime.INSTANCE.addBusinessJob(() -> {
            Path file = Paths.get(ROOT_PATH + ConfigLoader.DIR_PREPARE + path);
            try (FileWriter writer = new FileWriter(file.toString())) {
                writer.write(content);
            } catch (IOException e) {
                LOGGER.error("error to write content: {} to path: {}", content, path, e);
            }
        });
    }

    /**
     * 将配置文件归档，新的配置文件生效
     */
    public static Integer archive(String configName, int curVersion, Integer targetVersion) throws IOException {
        // 检查是否有新需要加载的文件
        String preparePath = ROOT_PATH + ConfigLoader.DIR_PREPARE;
        File prepareFile = new File(preparePath);

        String filePrefix = (targetVersion == null) ? configName : getFileName(configName, targetVersion.intValue());
        File[] files = prepareFile.listFiles((dir, name) -> name.startsWith(filePrefix));

        if (files == null || files.length == 0) {
            LOGGER.warn("no prepare file for config {}", configName);
            return null;
        }

        // 将现有的配置归档
        String archivePath = ROOT_PATH + ConfigLoader.DIR_ARCHIVE;
        Files.move(Paths.get(ROOT_PATH + configName),
                Paths.get(archivePath + getFileName(configName, curVersion)),
                StandardCopyOption.REPLACE_EXISTING);

        // 将新的配置生效
        File confFile = Stream.of(files)
                .sorted((file1, file2) -> {
                    String name1 = file1.getName();
                    Integer version1 = parseConfigVersion(name1);
                    String name2 = file2.getName();
                    Integer version2 = parseConfigVersion(name2);
                    return version2.compareTo(version1);
                })
                .findFirst().get();

        Files.copy(confFile.toPath(), Paths.get(ROOT_PATH + configName), StandardCopyOption.REPLACE_EXISTING);

        String name = confFile.toPath().toString();
        return parseConfigVersion(name);
    }

    public static void archiveAndDump(String configName, int curVersion, Configurable configBean) {
        ProxyRuntime.INSTANCE.addBusinessJob(() -> {
            String archivePath = ROOT_PATH + ConfigLoader.DIR_ARCHIVE;
            try {
                Files.move(Paths.get(ROOT_PATH + configName),
                        Paths.get(archivePath + getFileName(configName, curVersion)),
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                LOGGER.error("error to move file for config {}, version {}", configName, curVersion);
            }

            Path file = Paths.get(ROOT_PATH + configName);
            try (FileWriter writer = new FileWriter(file.toString())) {
                writer.write(dump(configBean));
            } catch (IOException e) {
                LOGGER.error("error to dump config to file, config name {}, version {}", configName, curVersion);
            }
        });
    }

    public static String getFileName(String configName, int version) {
        return configName + "-" + version;
    }

    private static Integer parseConfigVersion(String fileName) {
        return Integer.valueOf(fileName.substring(fileName.lastIndexOf("-") + 1));
    }

    /**
     * 创建指定的文件夹
     * @param directoryName
     * @return 返回创建的文件夹路径
     */
    public static String createDirectoryIfNotExists(String directoryName) throws IOException {
        String dirPath = ROOT_PATH + directoryName;
        Path directory = Paths.get(dirPath);
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
        return dirPath;
    }

    /**
     * 清空文件夹
     * @param directoryName
     * @param filePrefix
     * @throws IOException
     */
    public static void clearDirectory(String directoryName, String filePrefix) throws IOException {
        String dirPath = ROOT_PATH + directoryName;
        File dirFile = new File(dirPath);
        Stream.of(dirFile.listFiles())
                .filter(file -> {
                    if (filePrefix == null) {
                        return file != null;
                    } else {
                        return file != null && file.getName().startsWith(filePrefix);
                    }
                })
                .forEach(file -> file.delete());
    }
}
