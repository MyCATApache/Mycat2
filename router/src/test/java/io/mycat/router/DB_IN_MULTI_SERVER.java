package io.mycat.router;

import io.mycat.MycatException;
import io.mycat.router.routeResult.OneServerResultRoute;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jamie12221
 *  date 2019-05-18 11:09
 *
 * 根据sql中的表名进行路由,允许一个sql存在多个表名
 **/
public class DB_IN_MULTI_SERVER extends MycatRouterTest {

  final String module = this.getClass().getSimpleName();

  @Test
  public void happyPass() {
    MycatRouter router = loadModule(module);
    String sql = "select * from travelrecord;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    String sql2 = "select * from travelrecord2;";
    String schema2 = "db1";
    String dn2 = "dn2";
    ResultRoute result2 = router
                              .enterRoute(schema2, sql2);
    Assert.assertEquals(new OneServerResultRoute().setDataNodeOnce(dn2).setSqlOnce(sql2), result2);
  }

  @Test
  public void butSchema() {
    thrown.expect(MycatException.class);
    String sql = "select * from travelrecord;";
    String schema = "errorDb";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void butSQLNoSchema() {
    thrown.expect(MycatException.class);
    String sql = "select 1;";
    String schema = "db1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
  }

  @Test
  public void butSQLOtherSchema() {
    thrown.expect(MycatException.class);
    String sql = "select * from db2.travelrecord";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void multiSQL() {
    String sql = "select * from travelrecord;select * from travelrecord";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void multiSQLButTable() {
    thrown.expect(MycatException.class);
    String sql = "select * from travelrecord;select * from travelrecord2";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void multiSQLButSchema() {
    thrown.expect(MycatException.class);
    String sql = "select * from db1.travelrecord;select * from travelrecord2";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }
}
