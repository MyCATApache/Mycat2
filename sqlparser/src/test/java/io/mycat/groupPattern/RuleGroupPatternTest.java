package io.mycat.groupPattern;

import io.mycat.sqlparser.util.simpleParser2.GroupPattern;
import io.mycat.sqlparser.util.simpleParser2.GroupPatternBuilder;
import io.mycat.sqlparser.util.simpleParser2.GroupPatternException;
import io.mycat.sqlparser.util.simpleParser2.Matcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class RuleGroupPatternTest {
    @Test
    public void test() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT id FROM {table} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Assert.assertEquals("travelrecord", groupPattern.toContextMap(matcher).get("table"));
    }

    @Test
    public void test1() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT id FROM {table} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher(" id FROM travelrecord LIMIT 1;");
        Assert.assertFalse(matcher.acceptAll());
    }

    @Test
    public void test2() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT id FROM {table} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM `travelrecord` LIMIT 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Assert.assertEquals("`travelrecord`", groupPattern.toContextMap(matcher).get("table"));
    }

    @Test
    public void test3() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT {column} FROM {table} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("id", map.get("column"));
        Assert.assertEquals("travelrecord", map.get("table"));
    }

    @Test
    public void test4() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{type} id FROM {table} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("SELECT", map.get("type"));
        Assert.assertEquals("travelrecord", map.get("table"));
    }

    @Test
    public void test5() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{other}");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("select 1;", map.get("other"));
    }

    @Test
    public void test6() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{other} aaaa");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1; aaaa");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        ;
        Assert.assertEquals("select 1;", map.get("other"));
    }

    @Test
    public void test7() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("aaaa {other} ");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("aaaa select 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        ;
        Assert.assertEquals("select 1;", map.get("other"));
    }

    @Test
    public void test8() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {other} aaaa {other1} bbbb");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1; aaaa  select 2; bbbb");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        ;
        Assert.assertEquals("select 1;", map.get("other"));
        Assert.assertEquals("select 2;", map.get("other1"));
    }

    @Test
    public void test9() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {s1};{s2};{s3};");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1; select 2; select 3;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        ;
        Assert.assertEquals("select 1", map.get("s1"));
        Assert.assertEquals("select 2", map.get("s2"));
        Assert.assertEquals("select 3", map.get("s3"));
    }

    @Test
    public void test10() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {s1};;;;;{s2};{s3};");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1;;;;;select 2; select 3;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        ;
        Assert.assertEquals("select 1", map.get("s1"));
        Assert.assertEquals("select 2", map.get("s2"));
        Assert.assertEquals("select 3", map.get("s3"));
    }


    @Test(expected = GroupPatternException.NameAdjacentException.class)
    public void test11() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {s1}{s2};{s3};");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1 select 2; select 3;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("select 1", map.get("s1"));
        Assert.assertEquals("select 2", map.get("s2"));
        Assert.assertEquals("select 3", map.get("s3"));
    }

    @Test(expected = GroupPatternException.NameLocationAmbiguityException.class)
    public void test12() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT id FROM {table} LIMIT 1;");
        int id2 = patternBuilder.addRule("SELECT id FROM {table2} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
    }

    @Test
    public void test18() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT  id FROM {table} LIMIT 1;");
        int id2 = patternBuilder.addRule("{type} id FROM {table2} LIMIT 1;");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();

        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);

        Matcher matcher;
        Map<String, String> map;

        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals(null, map.get("type"));
        Assert.assertEquals("travelrecord", map.get("table"));


        matcher = groupPattern.matcher("select id FROM travelrecord LIMIT 1;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id2, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("select", map.get("type"));
        Assert.assertEquals("travelrecord", map.get("table2"));
    }

    @Test
    public void test19() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("LIMIT {count}");
        int id2 = patternBuilder.addRule("LIMIT 1");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);
        Assert.assertEquals(1, matcher.id());
        Assert.assertTrue(groupPattern.toContextMap(matcher).isEmpty());
    }

    @Test(expected = GroupPatternException.NameAmbiguityException.class)
    public void test20() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT  id FROM travelrecord LIMIT {count}");
        int id2 = patternBuilder.addRule("SELECT {count} FROM travelrecord LIMIT 1");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
    }

    @Test
    public void test21() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT  id FROM travelrecord LIMIT ");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertFalse(matcher.acceptAll());
    }

    @Test
    public void test22() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("  id FROM travelrecord LIMIT ");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertFalse(matcher.acceptAll());
    }

    @Test
    public void test23() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{any} SELECT  id FROM travelrecord LIMIT {any2}");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals(null, map.get("any"));
        Assert.assertEquals("1", map.get("any2"));
    }

    @Test
    public void test24() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{any} FROM travelrecord  {any2}");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Assert.assertEquals(id, matcher.id());
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("SELECT id", map.get("any"));
        Assert.assertEquals("LIMIT 1", map.get("any2"));
    }

    @Test
    public void test25() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{any} FROM travelrecord  {any2}");
        int id2 = patternBuilder.addRule("SELECT id FROM travelrecord  {any3}");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();

        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);


        Matcher matcher;
        Map<String, String> map;

        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals(id2, matcher.id());
        Assert.assertEquals(null, map.get("any"));
        Assert.assertEquals("LIMIT 1", map.get("any3"));

        matcher = groupPattern.matcher("select id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals(id, matcher.id());
        Assert.assertEquals("select id", map.get("any"));
        Assert.assertEquals("LIMIT 1", map.get("any2"));
    }

    @Test
    public void test26() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("{any} FROM travelrecord  {any2}");
        int id2 = patternBuilder.addRule("SELECT id FROM travelrecord  {any3}");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();

        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);

        Matcher matcher;
        Map<String, String> map;

        matcher = groupPattern.matcher("select id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("select id", map.get("any"));
        Assert.assertEquals("LIMIT 1", map.get("any2"));

        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id2, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT 1", map.get("any3"));
    }

    @Test(expected = GroupPatternException.NameAmbiguityException.class)
    public void test13() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {s1};{s1};{s3};");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1;select 2; select 3;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("select 1", map.get("s1"));
        Assert.assertEquals("select 3", map.get("s3"));
    }

    @Test(expected = GroupPatternException.NameSyntaxException.class)
    public void test14() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {s1 s2};{s3};");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();
        Matcher matcher = groupPattern.matcher("select 1; select 3;");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(0, id);
        Map<String, String> map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("select 1", map.get("s1"));
        Assert.assertEquals("select 3", map.get("s3"));
    }

    @Test(expected = GroupPatternException.NameSyntaxException.class)
    public void test15() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {");
    }

    @Test(expected = GroupPatternException.NameSyntaxException.class)
    public void test16() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule(" {n");
    }

    @Test
    public void test27() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT id FROM travelrecord  {any2} 1");
        int id2 = patternBuilder.addRule("SELECT id FROM travelrecord {any2} 2");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();


        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);

        Matcher matcher;
        Map<String, String> map;


        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT", map.get("any2"));

        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 2");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id2, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT", map.get("any2"));
    }

    @Test
    public void test28() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("SELECT id FROM travelrecord  {any2}");
        int id2 = patternBuilder.addRule("SELECT id FROM travelrecord {any2} 2");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();

        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);

        Matcher matcher;
        Map<String, String> map;

        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT 1", map.get("any2"));

        matcher = groupPattern.matcher("SELECT id FROM travelrecord LIMIT 2");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id2, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT", map.get("any2"));
    }

    @Test
    public void test29() {
        GroupPatternBuilder patternBuilder = new GroupPatternBuilder(0);
        int id = patternBuilder.addRule("travelrecord  {any2}");
        int id2 = patternBuilder.addRule("travelrecord {any2} 2");
        int id3 = patternBuilder.addRule("travelrecord {any2} 3");
        GroupPattern groupPattern = patternBuilder.createGroupPattern();

        Assert.assertEquals(0, id);
        Assert.assertEquals(1, id2);
        Assert.assertEquals(2, id3);


        Matcher matcher;
        Map<String, String> map;

        matcher = groupPattern.matcher("travelrecord LIMIT 4");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT 4", map.get("any2"));

        matcher = groupPattern.matcher("travelrecord LIMIT 2");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id2, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT", map.get("any2"));

        matcher = groupPattern.matcher("travelrecord LIMIT 3");
        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(id3, matcher.id());
        map = groupPattern.toContextMap(matcher);
        Assert.assertEquals("LIMIT", map.get("any2"));


    }
}