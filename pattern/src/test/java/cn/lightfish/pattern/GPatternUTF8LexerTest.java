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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPatternUTF8LexerTest {

    @Test
    public void init() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        recorder.load(Collections.emptySet());
        GPatternUTF8Lexer utf8Lexer = new GPatternUTF8Lexer(recorder);
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
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        Set<String> map = new HashSet<>();
        map.add("1234");
        map.add("abc");
        map.add("1a2b3c");
        map.add("`qac`");
        recorder.load(map);
        GPatternUTF8Lexer utf8Lexer = new GPatternUTF8Lexer(recorder);
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
    public void init3() {
        GPatternIdRecorder recorder = new GPatternIdRecorderImpl(false);
        Set<String> map = new HashSet<>();
        map.add("1234");
        map.add("abc");
        map.add("1a2b3c");
        map.add("`qac`");
        recorder.load(map);
        GPatternUTF8Lexer utf8Lexer = new GPatternUTF8Lexer(recorder);
        String text = "/* mycat:注释  */ 1234 abc // 注释 \n 1a2b3c -- 注释 \n 'a哈哈哈哈1' `qac`";
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
}
