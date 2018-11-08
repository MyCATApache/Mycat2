package io.mycat.util.csv;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;

/**
 * 普通 io 和 Mapped 方式只是输入源不同，处理逻辑相同，所以这里的测试大部分也适用于 MappedCsvReader
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
        CsvReader reader = DefaultCsvReader.from(Paths.get(filePath));
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
        reader.hashNext();
        reader.hashNext();
        List<byte[]> next = reader.next();
        String value = new String(next.get(1), utf8);
        Assert.assertEquals(value, "\"朱强\"");
    }

    @Test
    public void test4() throws IOException {
        CsvReader reader = DefaultCsvReader.fromString("id,姓名,年龄\r\n" +
                "1,\"朱强\",20\r\n" +
                "2,zhuqiang,19\r");
        print(reader, utf8);
    }

    @Test
    public void test5() throws IOException {
        CsvReader reader = DefaultCsvReader.fromString(",id,姓名,年龄\r\n" +
                "1,\"朱强\",20\r\n" +
                "2,zhuqiang,19");
        reader.hashNext();
        // [][id][姓名][年龄]
        List<byte[]> next = reader.next();
        Assert.assertEquals(next.get(0).length, 0);
    }

    @Test
    public void test6() throws IOException {
        CsvReader reader = DefaultCsvReader.fromString("id,姓名,年龄\r\n" +
                "2,zhuqiang,19,");
        reader.hashNext();
        reader.hashNext();
        // [2][zhuqiang][19]
        List<byte[]> next = reader.next();
        // 因为读取完逗号之后，后面没有可读数据了
        Assert.assertEquals(next.size(), 3);
    }

    /**
     * 列扩容测试
     * @throws IOException
     */
    @Test
    public void test7() throws IOException {
        CsvReaderConfig csvReaderConfig = new CsvReaderConfig();
        csvReaderConfig.setReadeBufferSize(10);
        csvReaderConfig.setColumnBufferSize(2);
        CsvReader reader = DefaultCsvReader.fromString("id,姓名,年龄\r\n" +
                        "2,zhuqiang,19,",
                csvReaderConfig);
        print(reader, utf8);
    }

    /**
     * 特殊字符解析
     */
    @Test
    public void test8() throws IOException {
        // csv 协议值："""id""","姓名","年龄"
        // 实际值 "id",姓名,年龄
        CsvReader reader = DefaultCsvReader.fromString(
                "\"\"\"id\"\"\",\"姓名\",\"年龄\"\r\n" +
                        "\"2\",\"zhuqiang\",\"19\"");
        print(reader, utf8);
    }

    /**
     * 特殊字符解析
     */
    @Test
    public void test9() throws IOException {
        // csv 协议值："""id""","姓名","年龄"
        // 实际值 "id",姓名,年龄
        CsvReader reader = DefaultCsvReader.fromString(
                "\"\"\"id\r\n\"\"\",\"姓名\",\"年龄\"\r\n" +
                        "\"2\",\"zhuqiang\",\"19\"\r\n");
        print(reader, utf8);
    }

    @Test
    public void test10() throws IOException {
        // 超级复杂的一列中是一个 html 网页，测试通过
        String path = "C:\\Users\\mrcode\\Desktop\\mycatcsv\\product_info.csv";
        CsvReader from = DefaultCsvReader.from(Paths.get(path));
        print(from, utf8);
    }

    @Test
    public void test11() throws IOException {
        // 文本边界和无文本边界混合模式："2",zhuqiang,19
        CsvReader reader = DefaultCsvReader.fromString(
                "id,姓名,年龄\r\n" +
                        "\"2\",zhuqiang,19");
        print(reader, utf8);
    }

    @Test
    public void test12() throws IOException {
        // csv 协议值："2"",",zhuqiang,19
        // 实际值 [2",] [zhuqiang] [19]
        CsvReader reader = DefaultCsvReader.fromString(
                "id,姓名,年龄\r\n" +
                        "\"2\"\",\", zhuqiang, 19");
        print(reader, utf8);
    }
}