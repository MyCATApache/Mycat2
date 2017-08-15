package io.mycat.mycat2.sqlparser;

//import com.alibaba.druid.sql.parser.Token;
import javafx.util.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by Administrator on 2017/2/13 0013.
 */
public class MatchMethodGenerator {

//    static Map<String, Token> map = new HashMap<String, Token>();
//
//    static {
//
//        map.put("ALL", Token.ALL);
//        map.put("ALTER", Token.ALTER);
//        map.put("AND", Token.AND);
//        map.put("ANY", Token.ANY);
//        map.put("AS", Token.AS);
//        map.put("ENABLE", Token.ENABLE);
//        map.put("DISABLE", Token.DISABLE);
//        map.put("ASC", Token.ASC);
//        map.put("BETWEEN", Token.BETWEEN);
//        map.put("BY", Token.BY);
//        map.put("CASE", Token.CASE);
//        map.put("CAST", Token.CAST);
//        map.put("CHECK", Token.CHECK);
//        map.put("CONSTRAINT", Token.CONSTRAINT);
//        map.put("CREATE", Token.CREATE);
//        map.put("DATABASE", Token.DATABASE);
//        map.put("DEFAULT", Token.DEFAULT);
//        map.put("COLUMN", Token.COLUMN);
//        map.put("TABLESPACE", Token.TABLESPACE);
//        map.put("PROCEDURE", Token.PROCEDURE);
//        map.put("FUNCTION", Token.FUNCTION);
//        map.put("DELETE", Token.DELETE);
//        map.put("DESC", Token.DESC);
//        map.put("DISTINCT", Token.DISTINCT);
//        map.put("DROP", Token.DROP);
//        map.put("ELSE", Token.ELSE);
//        map.put("EXPLAIN", Token.EXPLAIN);
//        map.put("EXCEPT", Token.EXCEPT);
//        map.put("END", Token.END);
//        map.put("ESCAPE", Token.ESCAPE);
//        map.put("EXISTS", Token.EXISTS);
//        map.put("FOR", Token.FOR);
//        map.put("FOREIGN", Token.FOREIGN);
//        map.put("FROM", Token.FROM);
//        map.put("FULL", Token.FULL);
//        map.put("GROUP", Token.GROUP);
//        map.put("HAVING", Token.HAVING);
//        map.put("IN", Token.IN);
//        map.put("INDEX", Token.INDEX);
//        map.put("INNER", Token.INNER);
//        map.put("INSERT", Token.INSERT);
//        map.put("INTERSECT", Token.INTERSECT);
//        map.put("INTERVAL", Token.INTERVAL);
//        map.put("INTO", Token.INTO);
//        map.put("IS", Token.IS);
//        map.put("JOIN", Token.JOIN);
//        map.put("KEY", Token.KEY);
//        map.put("LEFT", Token.LEFT);
//        map.put("LIKE", Token.LIKE);
//        map.put("LOCK", Token.LOCK);
//        map.put("MINUS", Token.MINUS);
//        map.put("NOT", Token.NOT);
//        map.put("NULL", Token.NULL);
//        map.put("ON", Token.ON);
//        map.put("OR", Token.OR);
//        map.put("ORDER", Token.ORDER);
//        map.put("OUTER", Token.OUTER);
//        map.put("PRIMARY", Token.PRIMARY);
//        map.put("REFERENCES", Token.REFERENCES);
//        map.put("RIGHT", Token.RIGHT);
//        map.put("SCHEMA", Token.SCHEMA);
//        map.put("SELECT", Token.SELECT);
//        map.put("SET", Token.SET);
//        map.put("SOME", Token.SOME);
//        map.put("TABLE", Token.TABLE);
//        map.put("THEN", Token.THEN);
//        map.put("TRUNCATE", Token.TRUNCATE);
//        map.put("UNION", Token.UNION);
//        map.put("UNIQUE", Token.UNIQUE);
//        map.put("UPDATE", Token.UPDATE);
//        map.put("VALUES", Token.VALUES);
//        map.put("VIEW", Token.VIEW);
//        map.put("SEQUENCE", Token.SEQUENCE);
//        map.put("TRIGGER", Token.TRIGGER);
//        map.put("USER", Token.USER);
//        map.put("WHEN", Token.WHEN);
//        map.put("WHERE", Token.WHERE);
//        map.put("XOR", Token.XOR);
//        map.put("OVER", Token.OVER);
//        map.put("TO", Token.TO);
//        map.put("USE", Token.USE);
//        map.put("REPLACE", Token.REPLACE);
//        map.put("COMMENT", Token.COMMENT);
//        map.put("COMPUTE", Token.COMPUTE);
//        map.put("WITH", Token.WITH);
//        map.put("GRANT", Token.GRANT);
//        map.put("REVOKE", Token.REVOKE);
//        // MySql procedure: add by zz
//        map.put("WHILE", Token.WHILE);
//        map.put("DO", Token.DO);
//        map.put("DECLARE", Token.DECLARE);
//        map.put("LOOP", Token.LOOP);
//        map.put("LEAVE", Token.LEAVE);
//        map.put("ITERATE", Token.ITERATE);
//        map.put("REPEAT", Token.REPEAT);
//        map.put("UNTIL", Token.UNTIL);
//        map.put("OPEN", Token.OPEN);
//        map.put("CLOSE", Token.CLOSE);
//        map.put("CURSOR", Token.CURSOR);
//        map.put("FETCH", Token.FETCH);
//        map.put("OUT", Token.OUT);
//        map.put("INOUT", Token.INOUT);
//    }

    static final byte[] shrinkCharTbl = new byte[96];//为了压缩hash字符映射空间，再次进行转义
    static void initShrinkCharTbl () {
        shrinkCharTbl[0] = 1;//从 $ 开始计算
        IntStream.rangeClosed('0', '9').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'0'+2));
        IntStream.rangeClosed('A', 'Z').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'A'+12));
        IntStream.rangeClosed('a', 'z').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'a'+12));
        shrinkCharTbl['_'-'$'] = (byte)38;
    }

    static void sqlKeyHastTest(String fileName, Function<String, Long> fun, long maskBit) {
        initShrinkCharTbl();

        IntStream.range(0, 54).forEach(x -> {
//            Map<Long, List<Token>> map = Stream.of(Token.values())
            Map<Long, List<String>> map = null;
            try {
                map = Files.lines(Paths.get(fileName))
                        .collect(Collectors.groupingBy((t) -> {
                                    long hash = fun.apply(t);
                                    /*String name = t;
                                    char size = (char)name.length();
                                    long seed = 41;

                                    for(int i=0; i<size; i++) {
                                        byte c = shrinkCharTbl[name.charAt(i)-'$'];
                                        //BKDRHash
                                        hash = hash * seed + c;
                                    };
                                    return (long)((hash & (0x1ffL << (long)x)) >> (long)x);*/

                                    return (long)((hash & (maskBit << (long)x)) >> (long)x);
    //                            return t.name().chars().sum();
                                }
                        ));
            } catch (IOException e) {
                e.printStackTrace();
            }
            Map.Entry<Long, List<String>> maxItem = map.entrySet().stream()
                    //.filter((k) -> k.getValue().size() < 3)
//                    .count();
                    .max((a, b) -> a.getValue().size()>b.getValue().size()?1:(a.getValue().size()==b.getValue().size()?0:-1))
                    .get();
            long count = map.entrySet().stream().count();

            long max = maxItem.getValue().size();
//                    if (count == 0) {
                System.out.println("result = "+x+" ; max repeat = "+max+" ; count = "+count);
//            }

//            System.out.println("result = "+x+" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//            map.entrySet().stream()
//                    .filter((k) -> k.getValue().size() > 1)
//                    .forEach((e) -> System.out.format("%d : %s %n", e.getKey(), e.getValue().toString()));

        });}

    static long genHash(char[] str) {
        int seed = 41;
        long hash = 0;
        for (char c: str) {
            //BKDRHash
            hash = hash * seed + shrinkCharTbl[c-'$'];
        }
        return hash;
    }

    //JSHash
    static int genHash2(char[] str) {
        int hash = 1315423911;
        for(char c: str) {
            hash ^= ((hash<<5)+shrinkCharTbl[c-'$']+(hash>>2));
        }
        return hash;
    }

    static boolean cmp(char[] str1, char[] str2) {
        if (str1.length == str2.length) {
            for (int i=0; i< str1.length; i++) {
                if (str1[i] != str2[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static int collideCount = 0;
    static long hashCollideTest(final List<Pair<Long, char[]>> sqlKeys, final ArrayList<Character> srcArray,
                                final int maxDepth, final int depth, final char[] str,
                                final long totalCount, final long count) {
        long newCount = count;
        for (Character c: srcArray) {
            str[depth] = c;
            if (depth < maxDepth -1)
                newCount = hashCollideTest(sqlKeys, srcArray, maxDepth, depth+1, str, totalCount, newCount);
            else {
                final long hash = genHash(str);
                sqlKeys.forEach(x -> {
                    if (x.getKey() == hash && !cmp(x.getValue(), str)) {
                        collideCount++;
                        System.out.println("Key '"+String.valueOf(x.getValue())+"' collides with '"+String.valueOf(str)+"' with hash : "+hash);
                    }
                });
                newCount++;
                if (newCount%10000000 == 1) {
                    Date now=new Date();
                    System.out.println(now.toLocaleString()+" progress : "+newCount+"/"+totalCount);
                }
            }
        }
        return newCount;
    }

//    static void run() {
//        ArrayList<Character> srcArray = new ArrayList<>();
//        IntStream.range('A', 'Z').forEach(c -> srcArray.add((char)c));
//        IntStream.range('0', '9').forEach(c -> srcArray.add((char)c));
//        srcArray.add('_');
//        srcArray.add('-');
//
//        List<Pair<Long, char[]>> sqlKeys = Stream.of(Token.values())
//                .filter(t -> t.name()!=null)
//                .map(x -> new Pair<>(genHash(x.name().toCharArray()), x.name().toCharArray()))
//                .collect(Collectors.toList());
//
//        collideCount = 0;
//        int maxDepth = 7;
//        long totalCount = srcArray.size();
//        for(int i=0; i<maxDepth-1; i++) totalCount *= srcArray.size();
//        char[] str = new char[maxDepth];
//        long count = hashCollideTest(sqlKeys, srcArray, maxDepth, 0, str, totalCount, 0);
//        Date now = new Date();
//        if (count != totalCount) {
//            System.out.println(now.toLocaleString()+" finished : "+count+"/"+totalCount+" collideCount="+collideCount);
//        } else {
//            System.out.println(now.toLocaleString()+" success!"+" collideCount="+collideCount);
//
//        }
//    }

    static int BKRDHash(String str) {
        int hash = 0;
        int seed = 131;
        for(char c: str.toCharArray()) {
            hash = hash*seed + c;
        }
        return hash;
    }

    static int RSHash(String str) {
        int b = 378551;
        int a = 63689;
        int hash = 0;
        for(char c: str.toCharArray()) {
            hash = hash*a + c;
            a *= b;
        }
        return hash;
    }

    static void test1() {
        String a = "abcdefghijklmnopqrstuvwxyz";
        String b = "abcdefghijklmnopqrstuvwxyz";
        System.out.println(a+" : "+RSHash(a));
        System.out.println(b+" : "+RSHash(b));
    }

    /**
     * final boolean isFromToken() {
     * return icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('M') &&
     * nextIsBlank();
     * }
     */
//    static void isXXXTokenGenerator() {
//        map.keySet().forEach((s) -> {
//            String keyword = s.toLowerCase();
//            final byte ICMask = (byte) 0xDF;//ignore case mask;
//            keyword = String.valueOf(keyword.charAt(0)).toUpperCase() + keyword.substring(1, keyword.length());
//            System.out.println(keyword.chars()
//                    .skip(1)
//                    .mapToObj((i) -> String.format("icNextCharIs('%s')", (char) (i & ICMask)))
//                    .collect(Collectors
//                            .joining("&&",
//                                    String.format("final boolean is%sToken() {\n        return ", keyword),
//                                    "&&\n                nextIsBlank();\n   }")));
//        });
//    }

    /**
     * final void skipReferencesToken() {
     * pos+="References".length();
     * //pos<sqlLength?//越界检查
     * }
     */
//    static void skipXXXTokenGenerator() {
//        map.keySet().forEach((s) -> {
//            String keyword = s.toLowerCase();
//            keyword = String.valueOf(keyword.charAt(0)).toUpperCase() + keyword.substring(1, keyword.length());
//            System.out.format("final void skip%sToken() {\npos+=%d;\n}%n", keyword, keyword.length());
//        });
//    }
    static void GenerateLongTokenHash(String fileName) {
        initShrinkCharTbl();
        try {
            Files.lines(Paths.get(fileName))
                    .filter(x -> x.length()>0)
                    .forEach(x -> {
//                System.out.format("    public static final int %-16s = 0x%04x%04x;%n", x, genHash2(x.toCharArray()) & 0xFFFF, x.length());
                System.out.format("    public static final long %-12s = 0x%xL;%n", x, genHash(x.toCharArray()));
            });
//            System.out.println("conflict count : "+count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void GenerateIntTokenHash(String fileName) {
        initShrinkCharTbl();
        try {
            Files.lines(Paths.get(fileName))
                    .filter(x -> x.length()>0)
                    .forEach(x -> {
                System.out.format("    public static final int %-16s = 0x%04x%04x;%n", x, genHash2(x.toCharArray()) & 0xFFFF, x.length());
//                        System.out.format("    public static final long %-12s = 0x%x;%n", x, genHash(x.toCharArray()));
                    });
//            System.out.println("conflict count : "+count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //isXXXTokenGenerator();
        //skipXXXTokenGenerator();
//        sqlKeyHastTest("sql_tokens.txt", s -> genHash(s.toCharArray()), 0x1FFL);
//        sqlKeyHastTest("minimal_sql_tokens.txt", s -> (long)genHash2(s.toCharArray()), 0x1FL);
//        sqlKeyHastTest("minimal_sql_tokens.txt", s -> genHash(s.toCharArray()), 0x3FL);
//        run();
//        test1();
        GenerateIntTokenHash("minimal_sql_tokens.txt");
//        GenerateLongTokenHash("sql_tokens.txt");
//        initShrinkCharTbl();
//        System.out.format("0x%xL;%n", genHash("dn1".toCharArray()));


    }
}
