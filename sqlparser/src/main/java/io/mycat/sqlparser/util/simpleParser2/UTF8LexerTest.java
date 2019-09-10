package io.mycat.sqlparser.util.simpleParser2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UTF8LexerTest {

    public static void main(String[] args) throws IOException {
//        String message = "SELECT * FROM `db1`.`mycat_sequence` LIMIT 0, 1000;";
        String format = "/*  9999*/ ''SELECT (10000.2+2,ssss,'hahah少时诵诗书') FROM (db1.`mycat_sequence222`) where = {5} 2019-7-7 LIMIT 0, 1000;";
        String message = "/*  9999*/ ''SELECT (10000.2+2,ssss,'hahah少时诵诗书') FROM (db1.`mycat_sequence222`) '2019-7-7' LIMIT 0, 1000;";
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        Stream<String> lines = Files.lines(Paths.get("D:\\newgit2\\mycat4\\Mycat2\\sqlparser\\src\\test\\resources\\sql_tokens.txt"));
        Map<String, String> collect = lines.map(i -> new String[]{i.toUpperCase(), i.toLowerCase()}).flatMap(i -> Stream.of(i)).collect(Collectors.toMap(k -> k, v -> v));
        IdRecorderImpl idRecorder = new IdRecorderImpl(true);
        idRecorder.load(collect);
        UTF8Lexer utf8Lexer = new UTF8Lexer();
        DFG dfg = new DFG.DFGImpl();
        utf8Lexer.init(ByteBuffer.wrap(bytes), 0, bytes.length, idRecorder);
        Token join = idRecorder.getToken("JOIN");

        while (utf8Lexer.nextToken()) {
            Token c = idRecorder.toCurToken();
            System.out.println(idRecorder.getDebugIdString());
        }
    }

}