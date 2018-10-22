package io.mycat.util.csv;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;

/**
 * ${desc}
 * @author zhuqiang
 * @version 1.0.1 2018/10/22 19:04
 * @date 2018/10/22 19:04
 * @since 1.0
 */
public class DefaultCsvReaderTest {
    private Charset gbk = Charset.forName("GBK");
    private Charset utf8 = Charset.forName("utf8");

    private void print(CsvReader reader, Charset charset) throws IOException {
        while (reader.hashNext()) {
            List<byte[]> next = reader.next();
            next.stream().map(i -> new String(i, charset)).forEach(i -> {
                System.out.print("[" + i + "]");
            });
            System.out.println("");
        }
    }

    /**
     * 无特殊符号的分析
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        String filePath = "C:/Users/mrcode/Desktop/mycatcsv/ansi.csv";
        CsvReader reader = DefaultCsvReader.fromFile(Paths.get(filePath));
        print(reader, gbk);
    }

    /**
     * 无特殊符号的分析
     */
    @Test
    public void test2() throws IOException {
        CsvReader reader = DefaultCsvReader.fromString("id,姓名,年龄\r\n" +
                                                               "1,朱强,20\r\n" +
                                                               "2,zhuqiang,19");
        print(reader, utf8);
    }

    @Test
    public void test3() throws IOException {
        CsvReader reader = DefaultCsvReader.fromString("id,姓名,年龄\r\n" +
                                                               "1,\"朱强\",20\r\n" +
                                                               "2,zhuqiang,19\r");
//        print(reader, utf8);
        reader.hashNext();
        reader.hashNext();
        List<byte[]> next = reader.next();
        String value = new String(next.get(1), utf8);
        Assert.assertEquals(value, "\"朱强\"");
    }
}