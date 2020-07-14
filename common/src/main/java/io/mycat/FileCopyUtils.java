package io.mycat;


import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class FileCopyUtils {
    @SneakyThrows
    public static void main(String[] args) {
        //mycat 源码目录
        Path sourcePath = Paths.get("xxx/Mycat2");
        //diff文件输出目录
        Path targetPath = Paths.get("xxx/tmpclient");
        List<String> mapping = new ArrayList<>();
        //git diff xxx xxx --name-only > name_only.txt
        Files.lines(Paths.get("xxx/name_only.txt")).forEach(i -> {
            Path actFilePath = sourcePath.resolve(i);
            if (!Files.exists(actFilePath)){
                mapping.add(i+"\t\t<===========\t\t"+"delete");
                return;
            }
            try {
                Path actTargetPath = targetPath.resolve(i);
                File file = actTargetPath.getParent().toFile();
                if(!file.exists() || !file.isDirectory()) {
                    file.mkdirs();
                }
                mapping.add(i+"\t\t<===========\t\t"+actTargetPath.toString().replace(targetPath.toString(),""));
                Files.copy(actFilePath, actTargetPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        });
        Files.write(sourcePath.resolve("mapping.txt"),mapping);
    }
}