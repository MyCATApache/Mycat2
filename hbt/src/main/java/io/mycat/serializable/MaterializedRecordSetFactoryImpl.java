package io.mycat.serializable;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.ServerConfig;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class MaterializedRecordSetFactoryImpl implements MaterializedRecordSetFactory {

    public final int DEFAULT_SWAP_THRESHOLD = 20000;
    private final Path tmpPath;

    @SneakyThrows
    public MaterializedRecordSetFactoryImpl() {
        String tempDirectory = null;
        if (MetaClusterCurrent.exist(ServerConfig.class)) {
            ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
            tempDirectory = serverConfig.getTempDirectory();
        }
        if (tempDirectory == null) {
            tempDirectory = Files.createTempDirectory("mycat").toAbsolutePath().toString();
        }
        this.tmpPath = Paths.get(tempDirectory);
    }

    @Override
    public OffHeapObjectList createFixedSizeRecordSet(int expectSize) {
        DiskArrayList diskArrayList;
        if (DEFAULT_SWAP_THRESHOLD < expectSize) {
            diskArrayList = new DiskArrayList(0, this.tmpPath, DefaultSerializer.INSTANCE);
        } else {
            diskArrayList = new DiskArrayList(DEFAULT_SWAP_THRESHOLD, this.tmpPath, DefaultSerializer.INSTANCE);
        }

        return new OffHeapObjectList() {
            @NotNull
            @Override
            public Iterator<Object[]> iterator() {
                return diskArrayList.iterator();
            }

            @Override
            public void addObjects(Object[] objects) {
                diskArrayList.add(objects);
            }

            @Override
            public void finish() {
                diskArrayList.finish();
            }

            @Override
            public void close() {
                diskArrayList.close();
            }
        };
    }

    @Override
    public OffHeapObjectList createRecordSet() {
        return createFixedSizeRecordSet(DEFAULT_SWAP_THRESHOLD);
    }
}
