package io.mycat.util.csv;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/10/21 22:49
 */
public class DefaultCsvReader extends CsvReader {
    private static Charset defaultCharset = Charset.forName("utf-8");
    private InputStream input;
    // 由于不允许操作  buffer.array() ,需要一个额外的数组来兼容 普通的 io 读取
    private byte[] bufferArray;

    private DefaultCsvReader(InputStream input) {
        this.input = input;
    }

    /**
     * 从文件读取
     * @param filePath
     * @return
     * @throws IOException
     */
    public static CsvReader fromFile(Path filePath) throws IOException {
        FileInputStream input = new FileInputStream(filePath.toString());
        DefaultCsvReader defaultCsvReader = new DefaultCsvReader(input);
        defaultCsvReader.init();
        return defaultCsvReader;
    }

    /**
     * 从字符串解析，一般多用于测试
     * @param csvStr
     * @return
     * @throws IOException
     */
    public static CsvReader fromString(String csvStr) throws IOException {
        return fromString(csvStr, defaultCharset);
    }

    public static CsvReader fromString(String csvStr, Charset charset) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(csvStr.getBytes(charset));
        DefaultCsvReader defaultCsvReader = new DefaultCsvReader(input);
        defaultCsvReader.init();
        return defaultCsvReader;
    }

    @Override
    protected void init() throws IOException {
        if (inited) {
            return;
        }
        int bufferSize = 10;
        buffer = ByteBuffer.allocate(bufferSize);
        bufferArray = new byte[bufferSize];
        columnBuffer = ByteBuffer.allocate(2);
        // 解决第一行数据获取的时候无 源数据的问题
        hasMoreData = doCheckDataLength();
        inited = true;
    }

    @Override
    protected boolean doCheckDataLength() throws IOException {
        int read = input.read(bufferArray);
        if (read == -1) {
            // 当数据没有的时候，需要考虑集中情况，到时候再来分析
            System.out.println("没有更多的数据了");
            return false;
        } else {
            buffer.clear();
            buffer.put(bufferArray, 0, read);
            buffer.flip();
            return true;
        }
    }
}
