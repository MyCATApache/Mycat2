package io.mycat.util.csv;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/10/21 23:04
 */
public class MappedCsvReader extends CsvReader {
    // 记录当前映射的位置
    private long fileChannelPosition;
    // 记录当前 映射的大小，每次读取数据（映射）都必须重新计算
    private long fileChannelCurrentSize;
    // 该文件一共需要映射几次，映射次数结束，则标识文件已经读取完毕
    private long mappedCount;
    private long currentMappedCount;
    private long totalFileSize;
    private FileChannel input;
    private Path filePath;

    public MappedCsvReader(Path filePath) throws IOException {
        this.filePath = filePath;
        init();
    }

    @Override
    protected void init() throws IOException {
        if (inited) {
            return;
        }
        // bufferSize 最大不能超过 Integer.MAX_VALUE
        int bufferSize = 10;
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
        columnBuffer = ByteBuffer.allocate(2);
        // 解决第一行数据获取的时候无 源数据的问题
        hasMoreData = getMoreData();
        inited = true;
    }

    @Override
    protected boolean getMoreData() throws IOException {
        if (mappedCount == currentMappedCount) {
            // 当数据没有的时候，需要考虑集中情况，到时候再来分析
            System.out.println("没有更多的数据了");
            // 清空buffer，让gc 回收释放资源
//            buffer.clear();
            return false;
        }

        if (mappedCount == currentMappedCount) {
            fileChannelCurrentSize = totalFileSize - fileChannelPosition;
        } else {
            fileChannelPosition = currentMappedCount * fileChannelCurrentSize;
        }
        buffer = input.map(FileChannel.MapMode.READ_ONLY, fileChannelPosition, fileChannelCurrentSize);
        currentMappedCount++;
        return true;
    }
}
