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
    @Test
    public void test1(String[] args) throws IOException {
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
}