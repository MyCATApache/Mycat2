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
package io.mycat.router;

import com.fulmicoton.multiregexp.MultiPattern;
import com.fulmicoton.multiregexp.MultiPatternSearcher;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.jsqlparser.JSQLParserException;

/*
yaml配置
annotations:
  - annotation:
      type: single
      name: annotation-name
      columnNameList: id
      columnValueList: ^[1-9]\d*$
      tableName: travelrecord
      additional:
        - param1: ^[1-9]\d*$
        - param2: 2

 1.先实现核心功能,即根据SQL和schema获取供路由使用的参数
 2.可不实现配置读取
 */

/**
 * @author jamie12221
 * @date 2019-04-28 21:38
 **/
public interface SingleDataNodeDynamicAnnotation2 {

  /**
   * 动态注解名字
   */
  String name();

  /**
   * 处理sql
   */
  void processSQL(CharSequence sql, String schema) throws ParseException;

  /**
   * 在执行processSQL之后调用,判断SQL是否符合单节点路由规则
   */
  boolean isSingleDataNodeSQL();

  /**
   * 获取分片规则字段名
   */
  String getColumnName();

  /**
   * 获取分片规则字段值 返回值实现hash,equals,是不可变对象
   */
  Object getColumnValue();

  /**
   * 获取分片规则表名
   */
  String getTableName();

  /**
   * 获取供路由规则的额外信息 返回的map是不可变对象
   */
  Map<String, Object> getAdditional();

  /**
   * 清理本对象的状态,动态注解对象结束使用
   */
  void reset();

  static String textCharacteristic(String patternName, String pattern) {
    return String.format("(?<%s>%s", patternName, pattern);
  }

  //SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));
  public static void main(String[] args) throws ParseException, JSQLParserException {

    Pattern compile = Pattern.compile(
        "((?<idExpr>(?:id = )(?<id>[0-9]))|(?<betweenExpr>(?: between )(?<left>[0-9])(?: and )(?<right>[0-9])))*");
    Matcher matcher1 = compile.matcher("id = 1 between 2 and 3");
    boolean b = matcher1.find();
    String group = matcher1.group();
    SQLTextCharacteristic textCharacteristic = new SQLTextCharacteristic(
        Arrays.asList("id = [0-9]",
            "between [0-9] and [0-9]",
            "id >= [0-9] and id \\<= [0-9]")
    );
    Set<String> dist = new HashSet<>();
    List<String> textCharacteristicList = new ArrayList<>();
    Map<Integer, SQLTextCharacteristic> sqlTextCharacteristicMap = new HashMap<>();
    List<String> patterns = textCharacteristic.patterns();

    int id = 0;
    for (int i = 0; i < patterns.size(); i++) {
      String pattern = patterns.get(i);
      if (dist.add(pattern)) {
        textCharacteristicList.add(pattern);
        if (i == 0) {
          sqlTextCharacteristicMap.put(id, textCharacteristic);
        } else {
          textCharacteristic.putIdOfCharacteristicPattern(id, pattern);
        }
        id++;
      } else {
        throw new RuntimeException();
      }
    }

    MultiPatternSearcher matcher = MultiPattern
                                       .of(textCharacteristicList)
                                       .searcher();
    String sql = "SELECT * FROM t where id = 1 between 1 and 9";
    MultiPatternSearcher.Cursor cursor = matcher.search(sql);
    Map<String, String> arg = new HashMap<>();
    SQLMatchResult result = new SQLMatchResult(sql);
    while (cursor.next()) {
      int index = cursor.match();   // array with the pattern id which match ends at this position
      String pattern = textCharacteristicList.get(index);
      int start = cursor.start();
      int end = cursor.end();
      arg.put(pattern, sql.substring(start, end));
    }
    result.setCharacteristicPositionList(arg);
  }
}
