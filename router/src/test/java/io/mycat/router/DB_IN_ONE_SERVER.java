package io.mycat.router;

import io.mycat.MycatException;
import io.mycat.router.routeResult.OneServerResultRoute;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jamie12221
 *  date 2019-05-06 00:22
 *
 *
 * 根据当前seesion状态的schema进行路由,对sql没有限制
 **/
public class DB_IN_ONE_SERVER extends MycatRouterTest {

  final String module = this.getClass().getSimpleName();

  @Test
  public void happyPass() {
    String sql = "select * from travelrecord;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(new OneServerResultRoute().setDataNode(dn1).setSql(sql), result);
  }

  @Test
  public void butSchema() {
    thrown.expect(MycatException.class);
    String sql = "select * from travelrecord;";
    String schema = "errorDb";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(new OneServerResultRoute().setDataNode(dn1).setSql(sql), result);
  }

  @Test
  public void butSQLNoSchema() {
    String sql = "select 1;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(new OneServerResultRoute().setDataNode(dn1).setSql(sql), result);
  }

  /**
   * 因为不对sql分析,选择session的schema
   */
  @Test
  public void butSQLOtherSchema() {
    thrown.expect(MycatException.class);
    String sql = "select * from db2.travelrecord";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(new OneServerResultRoute().setDataNode(dn1).setSql(sql), result);
  }


  @Test
  public void multiSQL() {
    String sql = "select 1;select 2;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNode(dn1).setSql(sql));
  }

}
