package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Match;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Matches;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.RootBean;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Schema;
import io.mycat.util.YamlUtil;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jamie on 2017/9/5.
 */

public class DynamicAnnotation {
  //  AnnotationSchemaList annotations;
    public static final String annotation_list = "annotations";
    public static final String schema_tag = "schema_tag";
    public static final String schema_name = "name";
    public static final String match_list = "matches";
    public static final String match_tag = "match";
    public static final String match_name = "name";
    public static final String match_state = "state";
    public static final String match_sqltype = "sqltype";
    public static final String match_where = "where";
    public static final String match_tables = "tables";
    public static final String match_actions = "actions";

    /**
   * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
   *
   * @param sqlType
   * @return
   */
  public Object get(int sqlType, String tableName) {
    return new Object();
  }


  //动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
  public static void main(String[] args) throws Exception {
    RootBean object =YamlUtil.load("annotations.yaml", RootBean.class);
    HashMap<DynamicAnnotationKey, String[]> table = new HashMap<>();
    Iterator<Schema> iterator = object.getAnnotations().stream().map((s) -> s.getSchema()).iterator();
    while (iterator.hasNext()) {
      Schema schema = iterator.next();
      String schemaName = schema.getName().trim();
      List<Matches> matchesList = schema.getMatches();
      for (Matches matche : matchesList) {
        Match match = matche.getMatch();
        SQLType type = SQLType.valueOf(match.getSqltype().toUpperCase().trim());
        DynamicAnnotationKey key = new DynamicAnnotationKey(
                schemaName,
                type,
                match.getTables().toArray(new String[match.getTables().size()]),
                match.getName());
        table.put(key, new String[]{key.toString()});//todo 还没写完
      }
    }
    System.out.println(table);
  }



}
