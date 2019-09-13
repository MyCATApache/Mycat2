package io.mycat.sqlparser.util.simpleParser2;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IdRecorderImplTest {

    @Test
    public void createCopyRecorder() {

    }

    @Test
    public void load() {
        IdRecorder recorder = new IdRecorderImpl(false);
        Map<String, Object> map = new HashMap<>();
        map.put("a", "1");
        map.put("bbb", "2");
        recorder.load(map);
        Token a = recorder.getConstToken("a");
        Assert.assertNotNull(a);
        Assert.assertEquals("a", a.getSymbol());
        Assert.assertEquals("1", a.getAttr());
    }

    @Test(expected = GroupPatternException.NonASCIICharsetConstTokenException.class)
    public void load2() {
        IdRecorder recorder = new IdRecorderImpl(false);
        Map<String, Object> map = new HashMap<>();
        map.put("非ascii编码", "1");
        recorder.load(map);
        Token a = recorder.getConstToken("非ascii编码");
        Assert.assertNotNull(a);
        Assert.assertEquals("a", a.getSymbol());
        Assert.assertEquals("1", a.getAttr());
    }

    @Test(expected = GroupPatternException.TooLongConstTokenException.class)
    public void load3() {
        IdRecorder recorder = new IdRecorderImpl(false);
        Map<String, Object> map = new HashMap<>();
        String sb = IntStream.range(0, 65).mapToObj(i -> "1").collect(Collectors.joining());
        map.put(sb, "1");
        recorder.load(map);
    }

    @Test
    public void append() {
        IdRecorder recorder = new IdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        recorder.append('a');
        recorder.append('z');
        recorder.endRecordTokenChar(2);
        Token token = recorder.createConstToken("1");
        Assert.assertNotNull(token);
        Assert.assertEquals("az", token.getSymbol());
        Assert.assertEquals("1", token.getAttr());
    }

    @Test(expected = GroupPatternException.TooLongConstTokenException.class)
    public void append1() {
        IdRecorder recorder = new IdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        IntStream.range(0, 65).mapToObj(i -> '1').forEach(recorder::append);
        recorder.endRecordTokenChar(65);
        Token token = recorder.createConstToken("1");
    }
    @Test(expected = GroupPatternException.TooLongConstTokenException.class)
    public void append2() {
        IdRecorder recorder = new IdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        IntStream.range(0, 65).mapToObj(i -> '1').forEach(recorder::append);
        recorder.endRecordTokenChar(65);
        Token token = recorder.createConstToken("1");
    }
    @Test
    public void startRecordTokenChar() {
    }

    @Test
    public void endRecordTokenChar() {
    }

    @Test
    public void isToken() {
    }

    @Test
    public void createConstToken() {
    }

    @Test
    public void toCurToken() {
    }
}