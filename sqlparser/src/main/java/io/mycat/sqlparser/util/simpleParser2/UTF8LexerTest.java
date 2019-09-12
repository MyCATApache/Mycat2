package io.mycat.sqlparser.util.simpleParser2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UTF8LexerTest {

    public static void main(String[] args) throws IOException {
//        String message = "SELECT * FROM `db1`.`mycat_sequence` LIMIT 0, 1000;";
//        String format = "/*  9999*/ ''SELECT (10000.2+2,ssss,'hahah少时诵诗书') FROM (db1.`mycat_sequence222`) where = {5} 2019-7-7 LIMIT 0, 1000;";
//        String message = "/*  9999*/ ''SELECT {} (10000.2+2,ssss,'hahah少时诵诗书') FROM (db1.`mycat_sequence222`) 1+2/3+4 '2019-7-7' LIMIT 0, 1000;";
        String message = "{name} SELECT {name1} , {name299} FROM `db1`.`travelrecord` LIMIT 0,;";
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        Stream<String> lines = Files.lines(Paths.get("D:\\newgit2\\mycat4\\Mycat2\\sqlparser\\src\\test\\resources\\sql_tokens.txt"));
        Map<String, Object> collect = lines.map(i -> i.trim()).map(i -> new String[]{i.toUpperCase(), i.toLowerCase()}).flatMap(i -> Stream.of(i)).distinct().collect(Collectors.toMap(k -> k, v -> v));

        GroupPatternBuilder patternBuilder = new GroupPatternBuilder();
        int id = patternBuilder.addRule("SELECT {name1} FROM `db1`.`travelrecord` LIMIT 0,;1111");
        int id2 = patternBuilder.addRule("SELECT 2,{name3} , {name4} FROM `db1`.{table} LIMIT 0,;222");

        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT 2, 1 , 3 FROM `db1`.`travelrecord` LIMIT 0,;");
        Map<String, String> context = groupPattern.toContextMap(matcher);
        System.out.println(matcher.id());
        context.entrySet().forEach(c -> System.out.println(c));
    }


}