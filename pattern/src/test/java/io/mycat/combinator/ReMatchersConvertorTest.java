package io.mycat.combinator;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

public class ReMatchersConvertorTest {
    @Test
    public void testCharArray() {
        Assert.assertTrue(ReMatchers.asPredicateForCharArray("^夏威夷$").test("夏威夷".toCharArray()));
    }

    @Test
    public void testCharBuffer() {
        Assert.assertTrue(ReMatchers.asPredicateForCharBuffer("^夏威夷$").test(CharBuffer.wrap("夏威夷")));
    }

    @Test
    public void testCharsetBytebuffer() {
        Charset charset = Charset.defaultCharset();
        Assert.assertTrue(ReMatchers.asPredicateForCharsetBytebuffer("^夏威夷$", charset).test(ByteBuffer.wrap("夏威夷".getBytes(charset))));

    }
}