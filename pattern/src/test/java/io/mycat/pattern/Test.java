package io.mycat.pattern;

import org.junit.Assert;

public class Test {
    public static void main(String[] args) {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        int id = patternBuilder.addRule("select  {any} ");
        int id1 = patternBuilder.addRule("select {any} for update");
        GPattern gPattern = patternBuilder.createGroupPattern();
        GPatternMatcher matcher = gPattern.matcher("select *  from db1.travelrecord order by id limit 50 offset 0");


        Assert.assertTrue(matcher.acceptAll());
        Assert.assertEquals(matcher.id(),id);

        GPatternMatcher matcher2 = gPattern.matcher("SELECT * FROM db1.travelrecord WHERE user_id = '中文' FOR UPDATE");
        Assert.assertTrue(matcher2.acceptAll());
        Assert.assertEquals(matcher2.id(),id1);
    }
}