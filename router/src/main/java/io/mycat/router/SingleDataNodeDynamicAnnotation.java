package io.mycat.router;

import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

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
public interface SingleDataNodeDynamicAnnotation {

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
//SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));
  public static void main(String[] args) throws ParseException {
    //加载动态注解
    SingleDataNodeDynamicAnnotation sdn = new SingleDataNodeDynamicAnnotation() {
      @Override
      public String name() {
        return "test";
      }

      @Override
      public void processSQL(CharSequence sql, String schema) throws ParseException {

      }

      @Override
      public boolean isSingleDataNodeSQL() {
        return true;
      }

      @Override
      public String getColumnName() {
        return "id";
      }

      @Override
      public Object getColumnValue() {
        return 1;
      }

      @Override
      public String getTableName() {
        return "travelrecord";
      }

      @Override
      public Map<String, Object> getAdditional() {
        return Collections.EMPTY_MAP;
      }

      @Override
      public void reset() {

      }
    };
    sdn.processSQL("SELECT id FROM travelrecord WHERE id = 1", "test");
    if (sdn.isSingleDataNodeSQL()) {
      String columnName = sdn.getColumnName();
      Object columnValue = sdn.getColumnValue();
      int hash = Objects.hash(columnName);
      String tableName = sdn.getTableName();
      Map<String, Object> additional = sdn.getAdditional();

      //routing
      sdn.reset();
    } else {
      throw new RuntimeException("Multi-node routing is not supported.");
    }
  }
}
