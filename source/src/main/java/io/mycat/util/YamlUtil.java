package io.mycat.util;

import io.mycat.mycat2.ConfigLoader;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
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
                .append(File.separator).append("source")
                .append(File.separator).append("target")
                .append(File.separator).append("classes")
                .append(File.separator);
            ROOT_PATH = sb.toString();
            LOGGER.debug("MYCAT_HOME is not set, set the default path: {}", ROOT_PATH);
        } else {
            ROOT_PATH = mycatHome.endsWith(File.separator) ? mycatHome + ConfigLoader.DIR_CONF :
                    mycatHome + File.separator + ConfigLoader.DIR_CONF;
            LOGGER.debug("mycat home: {}, root path: {}", mycatHome, ROOT_PATH);
        }
    }

    /**
     * 从指定的文件中加载配置
     * @param fileName 需要加载的文件名
     * @param clazz 加载后需要转换成的类对象
     * @return
     * @throws FileNotFoundException
     */
    public static <T> T load(String fileName, Class<T> clazz) throws FileNotFoundException {
        InputStreamReader fis = null;
        try {
            URL url = YamlUtil.class.getClassLoader().getResource(fileName);
            if (url != null) {
                Yaml yaml = new Yaml();
                fis = new InputStreamReader(new FileInputStream(url.getFile()), StandardCharsets.UTF_8);
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

    /**
     * 将对象dump成yaml格式的字符串
     * @param obj
     * @return
     */
    public static String dump(Object obj) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer();
        representer.addClassTag(obj.getClass(), Tag.MAP);
        Yaml yaml = new Yaml(representer, options);
        return yaml.dump(obj);
    }

    /**
     * 将对象dump成yaml格式并保存成指定文件，文件名格式：confName + "-" + version，如mycat.yml-1
     * @param confName
     * @param version
     * @param content
     */
    public static void dumpToFile(String confName, int version, String content) {
        ProxyRuntime.INSTANCE.addBusinessJob(() -> {
            StringBuilder sb = new StringBuilder();
            sb.append(ROOT_PATH).append(ConfigLoader.DIR_PREPARE).append(confName).append("-").append(version);
            Path file = Paths.get(sb.toString());
            try (FileWriter writer = new FileWriter(file.toString())) {
                writer.write(content);
            } catch (IOException e) {
                LOGGER.error("error to write content: {} to path: {}", content, sb.toString(), e);
            }
        });
    }

    /**
     * 将配置文件归档，新的配置文件生效
     */
    public static boolean archive(String configName, int curVersion, int targetVersion) throws IOException {
        // 检查是否有新需要加载的文件
        File prepareDir = new File(ROOT_PATH + ConfigLoader.DIR_PREPARE);
        String fileName = getFileName(configName, targetVersion);
        File[] files = prepareDir.listFiles((dir, name) -> name.equals(fileName));

        if (files == null || files.length == 0) {
            LOGGER.warn("no prepare file for config {}", configName);
            return false;
        }

        // 将现有的配置归档
        String archivePath = ROOT_PATH + ConfigLoader.DIR_ARCHIVE;
        Files.move(Paths.get(ROOT_PATH + configName), Paths.get(archivePath + getFileName(configName, curVersion)), StandardCopyOption.REPLACE_EXISTING);

        // 将新的配置生效
        Files.copy(files[0].toPath(), Paths.get(ROOT_PATH + configName), StandardCopyOption.REPLACE_EXISTING);
        return true;
    }

    /**
     * 将配置文件归档，内存中的对象dump到文件中
     * @param configBean
     * @param configName
     * @param curVersion
     */
    public static void archiveAndDumpToFile(Configurable configBean, String configName, int curVersion) {
        ProxyRuntime.INSTANCE.addBusinessJob(() -> {
            String archivePath = ROOT_PATH + ConfigLoader.DIR_ARCHIVE;
            try {
                Files.move(Paths.get(ROOT_PATH + configName), Paths.get(archivePath + getFileName(configName, curVersion)), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                LOGGER.error("error to move file for config {}, version {}", configName, curVersion);
            }

            try (FileWriter writer = new FileWriter(Paths.get(ROOT_PATH + configName).toString())) {
                writer.write(dump(configBean));
            } catch (IOException e) {
                LOGGER.error("error to dump config to file, config name {}, version {}", configName, curVersion + 1);
            }
        });
    }

    private static String getFileName(String configName, int version) {
        return configName + "-" + version;
    }

    /**
     * 创建指定的文件夹
     * @param directoryName
     * @return 返回创建的文件夹路径
     */
    public static void createDirectoryIfNotExists(String directoryName) throws IOException {
        String dirPath = ROOT_PATH + directoryName;
        Path directory = Paths.get(dirPath);
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
    }

    /**
     * 清空文件夹
     * @param directoryName 对应conf目录下的指定文件夹
     * @param filePrefix 删除指定前缀的文件，null 删除所有文件
     * @throws IOException
     */
    public static void clearDirectory(String directoryName, String filePrefix) throws IOException {
        File dirFile = new File(ROOT_PATH + directoryName);
        Stream.of(dirFile.listFiles()).filter(file -> {
                    if (filePrefix == null) {
                        // 删除所有文件
                        return file != null;
                    } else {
                        // 删除指定前缀的文件
                        return file != null && file.getName().startsWith(filePrefix);
                    }
                }).forEach(file -> file.delete());
    }
}
