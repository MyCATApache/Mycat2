package io.mycat;

import org.datayoo.moql.MoqlException;
import org.datayoo.moql.Selector;
import org.datayoo.moql.SelectorDefinition;
import org.datayoo.moql.engine.MoqlEngine;
import org.datayoo.moql.parser.MoqlParser;
import org.datayoo.moql.sql.SqlDialectType;
import org.datayoo.moql.translator.SqlTranslatorHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Sql2ElasticsearchTest {

    public static void main(String[] args) throws MoqlException {
        String sql = "select id,name from sys_user where id = 1";

        Map<String, Object> context = new HashMap<>();

        SelectorDefinition definition = MoqlParser.parseMoql(sql);
        Selector selector = MoqlEngine.createSelector(definition);
        String esDsl = SqlTranslatorHelper.translate2Sql(selector, SqlDialectType.ELASTICSEARCH,context);

        assert Objects.equals(
                esDsl,
                "{\n" +
                "  \"_source\": {\n" +
                "    \"includes\": [\n" +
                "      \"id\",\n" +
                "      \"name\"\n" +
                "    ]\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"match_all\": {}\n" +
                "  }\n" +
                "}");
    }
}