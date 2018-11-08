package io.mycat.util.csv;

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
public class MappedCsvReaderTest {
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

    @Test
    public void test1() throws IOException {
        String filePath = "C:/Users/mrcode/Desktop/mycatcsv/ansi.csv";
        CsvReader reader = new MappedCsvReader(Paths.get(filePath));

        while (reader.hashNext()) {
            List<byte[]> next = reader.next();
            next.stream().map(i -> new String(i, Charset.forName("GBK"))).forEach(i -> {
                System.out.print("[" + i + "]");
            });
            System.out.println("");
        }
    }

    @Test
    public void test10() throws IOException {
        // 超级复杂的一列中是一个 html 网页，测试通过
        String path = "C:\\Users\\mrcode\\Desktop\\mycatcsv\\product_info.csv";
        CsvReader from = new MappedCsvReader(Paths.get(path));
        print(from, utf8);
    }
}