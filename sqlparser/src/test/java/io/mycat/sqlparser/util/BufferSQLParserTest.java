package io.mycat.sqlparser.util;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
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
    Map<String, Object> map = context.getStaticAnnotation().toMapAndClear(new HashMap<>());
    Assert.assertEquals(map.get("runOnMaster"), 1L);
    Assert.assertEquals(map.get("a"), "'select a.* from customer'");
    Assert.assertEquals(map.get("b"), "a");
  }

  @Test
  public void testAnnotation2() {
    String sql = "/* mycat:runOnMaster = 1,a='select a.* from customer',b=a*/ select a.* from customer a where a.company_id=1; ";
    parser.parse(sql.getBytes(), context);
    assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
    Map<String, Object> map = context.getStaticAnnotation().toMapAndClear(new HashMap<>());
    Assert.assertEquals(map.get("runOnMaster"), 1L);
    Assert.assertEquals(map.get("a"), "'select a.* from customer'");
    Assert.assertEquals(map.get("b"), "a");
  }
}