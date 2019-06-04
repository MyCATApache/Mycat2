package io.mycat.sqlparser.util;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
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
  public void testAnnotationBalance() {
    String sql = "/*! mycat:runOnMaster = 1,a='select a.* from customer',b=3*/ select a.* from customer a where a.company_id=1; ";
    parser.parse(sql.getBytes(), context);
    assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> map1 = context.getStaticAnnotation().toMapAndClear(map);
  }

}