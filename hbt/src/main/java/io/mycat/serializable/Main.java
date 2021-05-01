package io.mycat.serializable;

import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class Main {
    public static void main(String[] args) throws IOException {
        int swapThreshold = 10000;
        Path tempFile = Files.createTempDirectory("");
        DiskArrayList<Object[]> diskArrayList = new DiskArrayList<>(swapThreshold, tempFile, new DiskArrayList.Serializer() {
            @Override
            @SneakyThrows
            public Object read(ExtendedDataInputStream oo) throws IOException {
                return oo.readObjects();
            }

            @Override
            @SneakyThrows
            public void write(Object object, ExtendedDataOutputStream oo) throws IOException {
                oo.writeObjects((Object[]) object);
            }
        });
        for (int i = 0; i <swapThreshold+1; i++) {
            diskArrayList.add(new Object[]{LocalDateTime.now(), LocalTime.now(),"111",1});
        }
        diskArrayList.finish();
        Iterator<Object[]> iterator = diskArrayList.iterator();
        long count = StreamSupport.stream(diskArrayList.spliterator(), false).count();
 StreamSupport.stream(diskArrayList.spliterator(), false).forEach(new Consumer<Object[]>() {
     @Override
     public void accept(Object[] c) {
         System.out.println(c);
     }
 });
        System.out.println();
    }
}
