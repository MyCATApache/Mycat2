package io.mycat.util.csv;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * MappedByteBuffer 的数据源输入解析；比普通 io 具有更高的性能
 * @author : zhuqiang
 * @date : 2018/10/21 23:04
 */
public class MappedCsvReader extends CsvReader {
    /** 记录当前映射的位置 */
    private long fileChannelPosition;
    /** 记录当前 映射的大小，每次读取数据（映射）都必须重新计算 */
    private long fileChannelCurrentSize;
    /** 该文件一共需要映射几次，映射次数结束，则标识文件已经读取完毕 */
    private long mappedCount;
    private long currentMappedCount;
    private long totalFileSize;
    private FileChannel input;
    private Path filePath;

    public MappedCsvReader(Path filePath) throws IOException {
        this(filePath, DefaultCsvReaderConfig.CONFIG);
    }

    public MappedCsvReader(Path filePath, CsvReaderConfig config) throws IOException {
        this.filePath = filePath;
        this.config = config;
        init();
    }

    @Override
    protected void init() throws IOException {
        if (inited) {
            return;
        }
        // bufferSize 最大不能超过 Integer.MAX_VALUE
        int bufferSize = config.getReadeBufferSize();
        RandomAccessFile source = new RandomAccessFile(filePath.toFile(), "r");
        input = source.getChannel();
        // 要计算文件需要映射几次
        totalFileSize = Files.size(filePath);
        fileChannelCurrentSize = totalFileSize;
        long eachCount = 1;
        if (totalFileSize > bufferSize) {
            eachCount = totalFileSize / bufferSize;
            if (totalFileSize % bufferSize != 0) {
                eachCount += 1;
            }
            fileChannelCurrentSize = bufferSize;
        }
        mappedCount = eachCount;
        columnBuffer = ByteBuffer.allocate(config.getColumnBufferSize());
        // 解决第一行数据获取的时候无 源数据的问题
        hasMoreData = getMoreData();
        inited = true;
    }

    @Override
    protected boolean getMoreData() throws IOException {
        if (mappedCount == currentMappedCount) {
            // 清空buffer，让gc 回收释放资源
            // buffer.clear();  待测试是否不会影响物理文件
            return false;
        }

        // 当是最后一次的时候 （currentMappedCount 从 0 开始）
        // 读取剩余的数据
        if (mappedCount == currentMappedCount + 1) {
            fileChannelCurrentSize = totalFileSize - fileChannelPosition;
        } else {
            fileChannelPosition = currentMappedCount * fileChannelCurrentSize;
        }
        buffer = input.map(FileChannel.MapMode.READ_ONLY, fileChannelPosition, fileChannelCurrentSize);
        currentMappedCount++;
        return true;
    }
}
