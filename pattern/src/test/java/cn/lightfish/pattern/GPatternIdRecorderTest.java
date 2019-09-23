/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPatternIdRecorderTest {

    @Test
    public void load() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        Set<String> map = new HashSet<>();
        map.add("a");
        map.add("bbb");
        recorder.load(map);
        GPatternToken a = recorder.getConstToken("a");
        Assert.assertNotNull(a);
        Assert.assertEquals("a", a.getSymbol());

        GPatternIdRecorder copyRecorder = recorder.createCopyRecorder();
        GPatternToken bbb = copyRecorder.getConstToken("bbb");
        Assert.assertNotNull(bbb);
        Assert.assertEquals("bbb", bbb.getSymbol());
    }

    @Test(expected = GPatternException.NonASCIICharsetConstTokenException.class)
    public void load2() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        Set<String> map = new HashSet<>();
        map.add("非ascii编码");
        recorder.load(map);
        GPatternToken a = recorder.getConstToken("非ascii编码");
        Assert.assertNotNull(a);
        Assert.assertEquals("a", a.getSymbol());
    }

    @Test(expected = GPatternException.TooLongConstTokenException.class)
    public void load3() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        Set<String> map = new HashSet<>();
        String sb = IntStream.range(0, 65).mapToObj(i -> "1").collect(Collectors.joining());
        map.add(sb);
        recorder.load(map);
    }

    @Test
    public void append() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        recorder.append('a');
        recorder.append('z');
        recorder.endRecordTokenChar(2);
        GPatternToken token = recorder.createConstToken("az");

        Assert.assertNotNull(token);
        Assert.assertEquals("az", token.getSymbol());

        recorder.startRecordTokenChar(1);
        recorder.append('a');
        recorder.append('z');
        recorder.endRecordTokenChar(3);

        GPatternToken curToken = recorder.toCurToken();
        Assert.assertEquals(1, curToken.getStartOffset());
        Assert.assertEquals(3, curToken.getEndOffset());
    }

    @Test(expected = GPatternException.TooLongConstTokenException.class)
    public void append1() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        String collect = IntStream.range(0, 65).mapToObj(i -> "1").collect(Collectors.joining());
        for (byte aByte : collect.getBytes()) recorder.append(aByte);
        recorder.endRecordTokenChar(65);
        GPatternToken token = recorder.createConstToken(collect);
    }

    @Test(expected = GPatternException.TooLongConstTokenException.class)
    public void append2() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        String collect = IntStream.range(0, 65).mapToObj(i -> "1").collect(Collectors.joining());
        for (byte aByte : collect.getBytes()) recorder.append(aByte);
        recorder.endRecordTokenChar(65);
        GPatternToken token = recorder.createConstToken(collect);
    }

    @Test
    public void append3() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        IntStream.range(0, 66).mapToObj(i -> '哈').forEach(recorder::append);
        recorder.endRecordTokenChar(66);
        GPatternToken token = recorder.toCurToken();

        Assert.assertNotNull(token);
        Assert.assertEquals(0, token.getStartOffset());
        Assert.assertEquals(66, token.getEndOffset());
    }

    @Test
    public void append4() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        recorder.startRecordTokenChar(0);
        IntStream.range(0, 66).mapToObj(i -> '哈').forEach(recorder::append);
        recorder.endRecordTokenChar(66);
        GPatternToken token = recorder.toCurToken();

        Assert.assertNotNull(token);
        Assert.assertEquals(0, token.getStartOffset());
        Assert.assertEquals(66, token.getEndOffset());
    }
}