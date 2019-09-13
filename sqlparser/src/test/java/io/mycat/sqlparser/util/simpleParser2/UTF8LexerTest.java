package io.mycat.sqlparser.util.simpleParser2;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UTF8LexerTest {

    @Test
    public void init() {
        IdRecorder recorder = new IdRecorderImpl(false);
        Map<String, Object> map = new HashMap<>();
        recorder.load(map);
        UTF8Lexer utf8Lexer = new UTF8Lexer(recorder);
        String text = "1234 abc 1a2b3c 'a哈哈哈哈1' `qac`";
        utf8Lexer.init(text);

        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("1234", utf8Lexer.getCurTokenString());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("abc", utf8Lexer.getCurTokenString());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("1a2b3c", utf8Lexer.getCurTokenString());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("'a哈哈哈哈1'", utf8Lexer.getCurTokenString());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("`qac`", utf8Lexer.getCurTokenString());
    }

    @Test
    public void init2() {
        IdRecorder recorder = new IdRecorderImpl(false);
        Map<String, Object> map = new HashMap<>();
        map.put("1234", "1");
        map.put("abc", "2");
        map.put("1a2b3c", "3");
        map.put("`qac`", "4");
        recorder.load(map);
        UTF8Lexer utf8Lexer = new UTF8Lexer(recorder);
        String text = "1234 abc 1a2b3c 'a哈哈哈哈1' `qac`";
        utf8Lexer.init(text);

        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("1234", utf8Lexer.getCurTokenString());
        Assert.assertEquals("1", recorder.toCurToken().getAttr());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("abc", utf8Lexer.getCurTokenString());
        Assert.assertEquals("2", recorder.toCurToken().getAttr());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("1a2b3c", utf8Lexer.getCurTokenString());
        Assert.assertEquals("3", recorder.toCurToken().getAttr());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("'a哈哈哈哈1'", utf8Lexer.getCurTokenString());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("`qac`", utf8Lexer.getCurTokenString());
        Assert.assertEquals("4", recorder.toCurToken().getAttr());
    }

    @Test
    public void init3() {
        IdRecorder recorder = new IdRecorderImpl(false);
        Map<String, Object> map = new HashMap<>();
        map.put("1234", "1");
        map.put("abc", "2");
        map.put("1a2b3c", "3");
        map.put("`qac`", "4");
        recorder.load(map);
        UTF8Lexer utf8Lexer = new UTF8Lexer(recorder);
        String text = "/* mycat:注释  */ 1234 abc // 注释 \n 1a2b3c -- 注释 \n 'a哈哈哈哈1' `qac`";
        utf8Lexer.init(text);

        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("1234", utf8Lexer.getCurTokenString());
        Assert.assertEquals("1", recorder.toCurToken().getAttr());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("abc", utf8Lexer.getCurTokenString());
        Assert.assertEquals("2", recorder.toCurToken().getAttr());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("1a2b3c", utf8Lexer.getCurTokenString());
        Assert.assertEquals("3", recorder.toCurToken().getAttr());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("'a哈哈哈哈1'", utf8Lexer.getCurTokenString());
        Assert.assertTrue(utf8Lexer.nextToken());
        Assert.assertEquals("`qac`", utf8Lexer.getCurTokenString());
        Assert.assertEquals("4", recorder.toCurToken().getAttr());
    }

    @Test
    public void nextToken() {
    }

    @Test
    public void hasChar() {
    }

    @Test
    public void peekChar() {
    }

    @Test
    public void nextChar() {
    }

    @Test
    public void getString() {
    }
}