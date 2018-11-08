package io.mycat.util.csv;

import java.nio.charset.Charset;

/**
 * csv 读取配置
 * @author zhuqiang
 * @date 2018/10/23 14:51
 */
public class CsvReaderConfig {
    private Charset charset = DefaultCsvReaderConfig.CHARSET;
    private int readeBufferSize = DefaultCsvReaderConfig.READE_BUFFER_SIZE;
    private int columnBufferSize = DefaultCsvReaderConfig.COLUMN_BUFFER_SIZE;

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public int getReadeBufferSize() {
        return readeBufferSize;
    }

    public void setReadeBufferSize(int readeBufferSize) {
        this.readeBufferSize = readeBufferSize;
    }

    public int getColumnBufferSize() {
        return columnBufferSize;
    }

    public void setColumnBufferSize(int columnBufferSize) {
        this.columnBufferSize = columnBufferSize;
    }
}
