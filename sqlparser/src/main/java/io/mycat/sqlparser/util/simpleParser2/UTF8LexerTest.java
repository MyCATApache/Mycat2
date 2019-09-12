package io.mycat.sqlparser.util.simpleParser2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
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
        Map<String, String> collect = lines.map(i -> i.trim()).map(i -> new String[]{i.toUpperCase(), i.toLowerCase()}).flatMap(i -> Stream.of(i)).distinct().collect(Collectors.toMap(k -> k, v -> v));

        IdRecorderImpl idRecorder = new IdRecorderImpl(true);
        idRecorder.load(collect);
        UTF8Lexer utf8Lexer = new UTF8Lexer(idRecorder);
        DFG.DFGImpl dfg = new DFG.DFGImpl();
        idRecorder.tmp.setLexer(utf8Lexer);
        addRule(bytes, idRecorder, utf8Lexer, dfg);
        idRecorder.tmp.setLexer(null);
        byte[] bytes1 = "( '哈哈' SELECT  1 , 2 FROM `db1`.`travelrecord` LIMIT 0,;".getBytes();
        utf8Lexer.init(ByteBuffer.wrap(bytes1), 0, bytes1.length);

        DFG.Matcher matcher = dfg.getMatcher();
        while (utf8Lexer.nextToken()){
            TokenImpl token = idRecorder.toCurToken();
            if(matcher.accept(token)){
              System.out.println("accept:"+token);
            }else {
                System.out.println("reject:"+token);
            }
        }
        System.out.println(matcher.acceptAll());
       Map<String,String>  res=  matcher.values(bytes1);
        System.out.println(res);
    }

    private static void addRule(byte[] bytes, IdRecorderImpl idRecorder, UTF8Lexer utf8Lexer, DFG.DFGImpl dfg) {
        utf8Lexer.init(ByteBuffer.wrap(bytes), 0, bytes.length);
        dfg.addRule(new Iterator<DFG.Seq>() {
            @Override
            public boolean hasNext() {
                return utf8Lexer.nextToken();
            }

            @Override
            public DFG.Seq next() {
                return idRecorder.createConstToken(null);
            }
        });
    }

}