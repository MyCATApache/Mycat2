package cn.lightfish.pattern;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPatternCollectorTest {
    Map<String, Collection<String>> infos = new HashMap<>();

    @Before
    public void setUp() {
        if (infos.isEmpty()) {
            addTable(infos, "db1", "user");
            addTable(infos, "db1", "info");
            addTable(infos, "db1", "app");
            addTable(infos, "db1", "333333");
            addTable(infos, "db2", "user2");
            addTable(infos, "db2", "info2");
            addTable(infos, "db2", "app2");
            addTable(infos, "db2", "3333332");
        }
    }

    public static void addTable(Map<String, Collection<String>> infos, String schemaName, String tableName) {
        Collection<String> set = infos.computeIfAbsent(schemaName, (s) -> new HashSet<>());
        set.add(tableName);
    }

    @Test
    public void test22222() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select tmp.uname,tmp.appname as appname \n" +
                "from  \n" +
                "( select user.name as uname ,app.name as appname ,info.app_id as app_id \n" +
                "  from user,info,app \n" +
                "  where user.uid =info.uid and info.app_id = app.id) as  tmp,333333 \n" +
                " \n" +
                "where tmp.app_id = info.app_id;";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> res = tableCollector.geTableMap();
        Assert.assertTrue(res.get("db1").contains("app"));
        Assert.assertTrue(res.get("db1").contains("user"));
        Assert.assertTrue(res.get("db1").contains("333333"));
        Assert.assertTrue(res.get("db1").contains("info"));
    }

    @Test
    public void test() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select * from user";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
    }

    @Test
    public void test1() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select * from db1.user";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
    }

    @Test
    public void test2() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select * from `db1`.user";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
    }

    @Test
    public void test3() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select * from `db1`.`user`";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
    }

    @Test
    public void test4() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select * from db1.`user`";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
    }

    @Test
    public void test6() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "SELECT u.id , u2.id FROM user u ,db2.user2 u2 ";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
        Assert.assertTrue(result.get("db2").contains("user2"));
    }

    @Test
    public void test7() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "SELECT u.id , u2.id FROM user u ,user2 u2 ";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
        Assert.assertNull(result.get("db2"));
    }

    @Test
    public void test8() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "SELECT u.id , u2.id FROM user u ,db2.user2 u2 ";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("user"));
        Assert.assertTrue(result.get("db2").contains("user2"));
    }

    @Test
    public void test9() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "SELECT u.id , u2.id,                           db1.info.id        FROM user u ,db2.user2 u2 ";
        int id = patternBuilder.addRule(message);

        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("info"));//in fact ,it is wrong
        Assert.assertTrue(result.get("db1").contains("user"));
        Assert.assertTrue(result.get("db2").contains("user2"));
    }
    @Test
    public void test5() {
        GPatternBuilder patternBuilder = new GPatternBuilder(0);
        String message = "select count(*) count from (select *\r\n"
                + "          from (select b.sales_count,\r\n" + "                       b.special_type,\r\n"
                + "                       a.prod_offer_id offer_id,\r\n"
                + "                       a.alias_name as offer_name,\r\n"
                + "                       (select c.attr_value_name\r\n"
                + "                          from attr_value c\r\n"
                + "                         where c.attr_id = '994001448'\r\n"
                + "                           and c.attr_value = b.special_type) as attr_value_name,\r\n"
                + "                       a.offer_type offer_kind,\r\n"
                + "                       a.offer_comments,\r\n" + "                       a.is_ge\r\n"
                + "                  from prod_offer a, special_offer b\r\n"
                + "                 where a.prod_offer_id = b.prod_offer_id\r\n"
                + "                   and (a.offer_type = '11' or a.offer_type = '10')\r\n"
                + "                   and (b.region_id = '731' or b.region_id = '10000000')\r\n"
                + "                   and a.status_cd = '10'\r\n"
                + "                   and b.special_type = '0'\r\n" + "                union all\r\n"
                + "                select b.sales_count,\r\n" + "                       b.special_type,\r\n"
                + "                       a.prod_offer_id offer_id,\r\n"
                + "                       a.alias_name as offer_name,\r\n"
                + "                       (select c.attr_value_name\r\n"
                + "                          from attr_value c\r\n"
                + "                         where c.attr_id = '994001448'\r\n"
                + "                           and c.attr_value = b.special_type) as attr_value_name,\r\n"
                + "                       a.offer_type offer_kind,\r\n"
                + "                       a.offer_comments,\r\n" + "                       a.is_ge\r\n"
                + "                  from prod_offer a, special_offer b\r\n"
                + "                 where a.prod_offer_id = b.prod_offer_id\r\n"
                + "                   and (a.offer_type = '11' or a.offer_type = '10')\r\n"
                + "                   and (b.region_id = '731' or b.region_id = '10000000')\r\n"
                + "                   and a.status_cd = '10'\r\n"
                + "                   and b.special_type = '1'\r\n"
                + "                   and exists (select 1\r\n"
                + "                          from prod_offer_channel l\r\n"
                + "                         where a.prod_offer_id = l.prod_offer_id\r\n"
                + "                           and l.channel_id = '11')\r\n"
                + "                   and not exists\r\n" + "                 (select 1\r\n"
                + "                          from product_offer_cat ml\r\n"
                + "                         where ml.prod_offer_id = a.prod_offer_id\r\n"
                + "                           and ml.offer_cat_type = '89')\r\n"
                + "                   and (exists (select 1\r\n"
                + "                                  from sales_restrication\r\n"
                + "                                 where object_id = a.prod_offer_id\r\n"
                + "                                   and domain_id = '1965868'\r\n"
                + "                                   and restrication_flag = '0'\r\n"
                + "                                   and domain_type = '19F'\r\n"
                + "                                   and state = '00A') or exists\r\n"
                + "                        (select 1\r\n"
                + "                           from sales_restrication\r\n"
                + "                          where object_id = a.prod_offer_id\r\n"
                + "                            and domain_id = '843073100000000'\r\n"
                + "                            and restrication_flag = '0'\r\n"
                + "                            and domain_type = '19E'\r\n"
                + "                            and state = '00A') or exists\r\n"
                + "                        (select 1\r\n"
                + "                           from sales_restrication\r\n"
                + "                          where object_id = a.prod_offer_id\r\n"
                + "                            and domain_id = '1965868'\r\n"
                + "                            and restrication_flag = '0'\r\n"
                + "                            and domain_type = '19X'\r\n"
                + "                            and state = '00A'\r\n"
                + "                            and (max_value = 1 or min_value = 1)\r\n"
                + "                            and extended_field = '731') or exists\r\n"
                + "                        (select 1\r\n"
                + "                           from sales_restrication\r\n"
                + "                          where object_id = a.prod_offer_id\r\n"
                + "                            and domain_id = concat('-', '11')\r\n"
                + "                            and restrication_flag = '0'\r\n"
                + "                            and domain_type = '19F'\r\n"
                + "                            and state = '00A') or not exists\r\n"
                + "                        (select 1\r\n"
                + "                           from sales_restrication\r\n"
                + "                          where object_id = a.prod_offer_id\r\n"
                + "                            and restrication_flag = '0'\r\n"
                + "                            and (domain_type in ('19F', '19E') or\r\n"
                + "                                (domain_type = '19X' and\r\n"
                + "                                extended_field = '731' and\r\n"
                + "                                (max_value = 1 or min_value = 1)))\r\n"
                + "                            and state = '00A'))\r\n"
                + "                   and not exists (select 1\r\n"
                + "                          from sales_restrication\r\n"
                + "                         where object_id = a.prod_offer_id\r\n"
                + "                           and domain_id = '1965868'\r\n"
                + "                           and restrication_flag = '1'\r\n"
                + "                           and domain_type = '19F'\r\n"
                + "                           and state = '00A')\r\n"
                + "                   and not exists (select 1\r\n"
                + "                          from sales_restrication\r\n"
                + "                         where object_id = a.prod_offer_id\r\n"
                + "                           and domain_id = '843073100000000'\r\n"
                + "                           and restrication_flag = '1'\r\n"
                + "                           and domain_type = '19E'\r\n"
                + "                           and state = '00A')\r\n"
                + "                   and not exists\r\n" + "                 (select 1\r\n"
                + "                          from sales_restrication\r\n"
                + "                         where object_id = a.prod_offer_id\r\n"
                + "                           and domain_id = '1965868'\r\n"
                + "                           and restrication_flag = '1'\r\n"
                + "                           and domain_type = '19X'\r\n"
                + "                           and state = '00A'\r\n"
                + "                           and (min_value = 1 or max_value = 1)\r\n"
                + "                           and extended_field = '731')\r\n"
                + "                   and not exists (select 1\r\n"
                + "                          from sales_restrication\r\n"
                + "                         where object_id = a.prod_offer_id\r\n"
                + "                           and domain_id = concat('-', '11')\r\n"
                + "                           and restrication_flag = '1'\r\n"
                + "                           and domain_type = '19F'\r\n"
                + "                           and state = '00A')\r\n" + "                   and exists\r\n"
                + "                 (select 1\r\n" + "                          from prod_offer_region v1\r\n"
                + "                         where v1.prod_offer_id = a.prod_offer_id\r\n"
                + "                           and (v1.common_region_id = '731' or\r\n"
                + "                               v1.common_region_id = '10000000' or\r\n"
                + "                               v1.common_region_id = '73101'))) t\r\n"
                + "         order by t.sales_count desc)";
        int id = patternBuilder.addRule(message);
        addTable(infos, "db1", "prod_offer");
        addTable(infos, "db1", "special_offer");
        addTable(infos, "db1", "sales_restrication");
        addTable(infos, "db1", "prod_offer_region");
        addTable(infos, "db1", "sales_restrication");
        addTable(infos, "db1", "product_offer_cat");
        GPatternIdRecorder recorder = patternBuilder.geIdRecorder();
        TableCollectorBuilder builder = new TableCollectorBuilder(recorder, infos);
        TableCollector tableCollector = builder.create();
        GPattern gPattern = patternBuilder.createGroupPattern(tableCollector);

        tableCollector.useSchema("db1");
        GPatternMatcher matcher = gPattern.matcherAndCollect(message);
        Assert.assertTrue(matcher.acceptAll());
        Map<String, Collection<String>> result = tableCollector.geTableMap();
        Assert.assertTrue(result.get("db1").contains("prod_offer"));
        Assert.assertTrue(result.get("db1").contains("special_offer"));
        Assert.assertTrue(result.get("db1").contains("sales_restrication"));
        Assert.assertTrue(result.get("db1").contains("prod_offer_region"));
        Assert.assertTrue(result.get("db1").contains("sales_restrication"));
        Assert.assertTrue(result.get("db1").contains("product_offer_cat"));
    }


}