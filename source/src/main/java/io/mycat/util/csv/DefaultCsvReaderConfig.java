package io.mycat.util.csv;

import java.nio.charset.Charset;

/**
 * <pre>
 * csv 解析默认配置，可以在项目启动时全局配置本类属性，
 * 当不自定义配置的时候，也相当于是全局修改了默认配置
 *
 * </pre>
 * @author zhuqiang
 * @date 2018/10/23 14:38
 */
public class DefaultCsvReaderConfig {
    /** 默认字符集，解析中是不需要的，调试需要 */
    public static Charset CHARSET = Charset.forName("utf-8");

    /** 从文件读取数据的 buffer 大小：缓冲区大小可以提高性能 */
    public static int READE_BUFFER_SIZE = 1024;

    /**
     * <pre>
     * 一列数据的缓冲区大小，当 READE_BUFFER_SIZE 大于一列数据的时候，不会使用该 列buffer
     * 这将会提高性能，举个例子：
     *      READE_BUFFER_SIZE = 100；一个 csv 文件的所有列的值 都小于 100
     *      那么 列buffer 用到的次数会大大减少,减少数据的 copy 操作；
     * 当不够装下一列值得时候就会触发扩容操作,如果该值设置得不合适的话，会造成好几次的扩容操作
     * </pre>
     */
    public static int COLUMN_BUFFER_SIZE = 1024;

    /** 默认配置 */
    public static CsvReaderConfig CONFIG = new CsvReaderConfig();

}
