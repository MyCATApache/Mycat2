package io.mycat;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ZipUtil {

    public static void main(String[] args) throws Exception{
        JarFile jarFile = new JarFile(
                ""
        );
        Map<String, JarEntry> map= new HashMap<>();
        jarFile.stream().forEach(new Consumer<JarEntry>() {
            @Override
            public void accept(JarEntry jarEntry) {
                if(!map.containsKey(jarEntry.getName())){
                    map.put(jarEntry.getName(),jarEntry);
                }else {
                    System.out.println();
                }

            }
        });
        System.out.println();
    }
}
