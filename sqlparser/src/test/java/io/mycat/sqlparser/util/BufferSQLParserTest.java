package io.mycat.sqlparser.util;

import static org.junit.Assert.assertEquals;

import io.mycat.sqlparser.util.simpleParser.BufferSQLContext;
import io.mycat.sqlparser.util.simpleParser.BufferSQLParser;
import io.mycat.sqlparser.util.simpleParser.MatchMethodGenerator;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BufferSQLParserTest {

  //    SQLParser parser;
  BufferSQLParser parser;
  BufferSQLContext context;

  @Before
  public void setUp() throws Exception {
    parser = new BufferSQLParser();
    context = new BufferSQLContext();
    //parser.init();
    MatchMethodGenerator.initShrinkCharTbl();
  }

  /**
   *
   */
  @Test
  public void testAnnotation() {
    String sql = "/*! mycat:runOnMaster = 1,a='select a.* from customer',b=a*/ select a.* from customer a where a.company_id=1; ";
    parser.parse(sql.getBytes(), context);
    assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
    HashMap<String, Object> map = new HashMap<>();
    context.getStaticAnnotation().toMapAndClear(new SQLMapAnnotation.PutKeyValueAble() {
      @Override
      public void put(String key, long value) {
        (map).put(key, value);
      }

      @Override
      public void put(String key, String value) {
        (map).put(key, value);
      }
    });
    Assert.assertEquals(map.get("runOnMaster"), 1L);
    Assert.assertEquals(map.get("a"), "'select a.* from customer'");
    Assert.assertEquals(map.get("b"), "a");
  }

  @Test
  public void testAnnotation2() {
    String sql = "/* mycat:runOnMaster = 1,a='select a.* from customer',b=a*/ select a.* from customer a where a.company_id=1; ";
    parser.parse(sql.getBytes(), context);
    assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
    HashMap<String, Object> map = new HashMap<>();
    context.getStaticAnnotation().toMapAndClear(new SQLMapAnnotation.PutKeyValueAble() {
      @Override
      public void put(String key, long value) {
        (map).put(key, value);
      }

      @Override
      public void put(String key, String value) {
        (map).put(key, value);
      }
    });
    Assert.assertEquals(map.get("runOnMaster"), 1L);
    Assert.assertEquals(map.get("a"), "'select a.* from customer'");
    Assert.assertEquals(map.get("b"), "a");
  }
  @Test
  public void testAnnotation3() {
    String sql = "/* mycat:schema = test2 */ select * from travelrecord; ";
    parser.parse(sql.getBytes(), context);
    HashMap<String, Object> map = new HashMap<>();
    context.getStaticAnnotation().toMapAndClear(new SQLMapAnnotation.PutKeyValueAble() {
      @Override
      public void put(String key, long value) {
        (map).put(key, value);
      }

      @Override
      public void put(String key, String value) {
        (map).put(key, value);
      }
    });
    System.out.println();
  }
}