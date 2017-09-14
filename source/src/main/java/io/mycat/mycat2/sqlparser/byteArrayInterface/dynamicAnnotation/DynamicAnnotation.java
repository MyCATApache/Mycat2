package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Match;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Matches;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.RootBean;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Schema;
import io.mycat.util.YamlUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        String state = match.getState();
        if (state == null) continue;
        if (!state.trim().toUpperCase().equals("OPEN")) continue;
        SQLType type = SQLType.valueOf(match.getSqltype().toUpperCase().trim());
        DynamicAnnotationKey key = new DynamicAnnotationKey(
                schemaName,
                type,
                match.getTables().toArray(new String[match.getTables().size()]),
                match.getName());
        List<Map<String, String>> conditionList = match.getWhere();
//       Map<Boolean, List<Map<String, String>>> map=

        Map<Boolean, List<Map<String, String>>> map =
                conditionList.stream().collect(Collectors.partitioningBy((p) -> {
                  String string = mappingKey(p).toUpperCase().trim();
                  return "AND".equals(string);
                }));
        Map<Boolean, List<String>> resMap = new HashMap<>();
        resMap.put(Boolean.TRUE, map.get(Boolean.TRUE).stream().map((m) -> mappingValue(m)).distinct().collect(Collectors.toList()));
        resMap.put(Boolean.FALSE, map.get(Boolean.FALSE).stream().map((m) -> mappingValue(m)).distinct().collect(Collectors.toList()));

        code(resMap);
        table.put(key, new String[]{key.toString()});//todo
      }
    }
    System.out.println(table);
  }

  private static String code(Map<Boolean, List<String>> map) {
    List<String> andList = map.get(Boolean.TRUE);
    String andString = "";
    String orString = "";
    if (andList != null && andList.size() != 0) {
      andString = codeAnd(andList.iterator());
    }
    List<String> orList = map.get(Boolean.FALSE);
    if (orList != null && orList.size() != 0) {
    orString=  codeOr(orList.iterator());
    }
    System.out.println(andString);
    System.out.println(orString);
    return andString+orString;
  }

  private static String codeAnd(Iterator<String> iterator) {
    if (iterator.hasNext()) {
      return genIfElse(iterator.next(), codeAnd(iterator));
    } else {
      return "\nreturn ture;";
    }
  }

  private static String codeOr(Iterator<String> iterator) {
    StringBuilder stringBuilder = new StringBuilder();
    while (iterator.hasNext()) {
      stringBuilder.append(genIfElse(iterator.next(), "\nreturn ture;"));
      stringBuilder.append("\nreturn ture;");
    }
    return stringBuilder.toString();
  }

  private static String genIfElse(String condition, String body) {
    return "\nif(" + condition + "){\n" + body + "\n}\n";
  }

  private final static String mappingKey(Map<String, String> i) {
    String res = i.get("and");
    if (res != null) {
      return "and";
    }
    res = i.get("AND");
    if (res != null) {
      return "AND";
    }
    res = i.get("or");
    if (res != null) {
      return  "or";
    }
    res = i.get("OR");
    if (res != null) {
     return  "OR";
    }

    return "";
  }

  private final static String mappingValue(Map<String, String> i) {
    if (i.size() != 1) {
      //todo 日志
      return "";
    } else {
      return i.values().iterator().next();
    }
  }




}
