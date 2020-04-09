package io.mycat.combinator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReMatchersTest {
    private Matcher foo;

    @Before
    public void setUp() throws Exception {
        foo = (abc, abc1) -> ReMatchers.asPredicateForString(abc).test(abc1);
    }

    interface Matcher{
        boolean match(String abc, String abc1);
    }

    @Test
    public void testAnd() {
        assertEquals(true, foo.match("abc", "abc"));
    }

    @Test
    public void testCaret () {
        assertEquals(true, foo.match("^a", "abc"));
    }

    @Test
    public void testCaret2() {
        assertEquals(false, foo.match("^b", "abc"));
    }

    @Test
    public void testMidMatch() {
        assertEquals(true, foo.match("abc", "aaabcbbcc"));
    }

    @Test
    public void testDollar() {
        assertEquals(true, foo.match("c$", "aaabcbbcc"));
    }

    @Test
    public void testWildcard() {
        assertEquals(true, foo.match(".*cc", "aaabcbbcc"));
    }

    @Test
    public void testIllegal() {
        assertEquals(false, foo.match("jkl", "aaabcbbcc"));
    }
    @Test
    public void testMidAny() {
        assertEquals(true, foo.match("cb.", "aaabcbbcc"));
    }

    @Test
    public void testIllegal2() {
        assertEquals(false, foo.match("cb.", "aaabcabcc"));
    }
    @Test
    public void testIllegal3() {
        assertEquals(true, foo.match("c+", "aaabcabcc"));
    }
    @Test
    public void testPlus() {
        assertEquals(true, foo.match("c+", "c"));
    }
    @Test
    public void testmPlus() {
        assertEquals(false, foo.match("a+", "c"));
    }


}