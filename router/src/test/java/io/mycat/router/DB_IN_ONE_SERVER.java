package io.mycat.router;

import io.mycat.MycatExpection;
import io.mycat.router.routeResult.OneServerResultRoute;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jamie12221
 * @date 2019-05-06 00:22
 **/
public class DB_IN_ONE_SERVER extends MycatRouterTest {

  final   String module = this.getClass().getSimpleName();

  @Test
  public void happyPass() {
    String sql = "select * from travelrecord;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNode(dn1).setSql(sql));
  }

  @Test
  public void butSchema() {
    thrown.expect(MycatExpection.class);
    String sql = "select * from travelrecord;";
    String schema = "errorDb";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNode(dn1).setSql(sql));
  }

  @Test
  public void butSQLNoSchema() {
    String sql = "select 1;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNode(dn1).setSql(sql));
  }

  @Test
  public void butSQLOtherSchema() {
    String sql = "select * from db2.travelrecord";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNode(dn1).setSql(sql));
  }

  @Test
  public void butDataNode() {
    String sql = "select 1;";
    String schema = "db1";
    String dn2 = "dn2";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNode("dn1").setSql(sql));
    Assert.assertNotEquals(result, new OneServerResultRoute().setDataNode(dn2).setSql(sql));
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
