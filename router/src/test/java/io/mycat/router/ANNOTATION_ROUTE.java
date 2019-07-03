package io.mycat.router;

import io.mycat.MycatException;
import io.mycat.router.routeResult.GlobalTableWriteResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jamie12221
 *  date 2019-05-18 12:42 使用动态注解对sql中的值提取进行路由,为了防止过多情况,限制sql只能有一个表名 全局表不进行动态注解影响
 **/
public class ANNOTATION_ROUTE extends MycatRouterTest {

  final String module = this.getClass().getSimpleName();

  @Test
  public void SHARING_DATABASE_EQUAL() {
    MycatRouter router = loadModule(module);
    String sql;
    String schema;
    String dn1;
    schema = "db1";
    sql = "SELECT * FROM `travelrecord` WHERE id = 0";
    dn1 = "dn1";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    sql = "SELECT * FROM `travelrecord` WHERE id = 256";
    dn1 = "dn2";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    sql = "SELECT * FROM `travelrecord` WHERE id = 512";
    dn1 = "dn3";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    sql = "SELECT * FROM `travelrecord` WHERE id = 1023";
    dn1 = "dn4";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void SHARING_DATABASE_RANGE() {
    MycatRouter router = loadModule(module);
    String sql;
    String schema;
    String dn1;
    schema = "db1";
    sql = "SELECT * FROM `travelrecord` WHERE id BETWEEN 1 AND 255;";
    dn1 = "dn1";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    sql = "SELECT * FROM `travelrecord` WHERE id BETWEEN 256 AND 511;";
    dn1 = "dn2";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    sql = "SELECT * FROM `travelrecord` WHERE id BETWEEN 512 AND 767;";
    dn1 = "dn3";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));

    sql = "SELECT * FROM `travelrecord` WHERE id BETWEEN 768 AND 1023;";
    dn1 = "dn4";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void SHARING_DATABASE_EQUAL_RANGE() {
    MycatRouter router = loadModule(module);
    String sql;
    String schema;
    String dn1;
    schema = "db1";
    sql = "SELECT * FROM `travelrecord` WHERE id BETWEEN 1 AND 255 AND id = 5;";
    dn1 = "dn1";
    Assert.assertEquals(router.enterRoute(schema, sql),
        new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  /**
   * 全局表
   */
  @Test
  public void GLOBAL() {
    MycatRouter router = loadModule(module);
    String sql;
    String schema;
    String dn1;
    schema = "db1";
    sql = "SELECT * FROM `travelrecord2`";
    dn1 = "dn1";
    ResultRoute resultRoute = router.enterRoute(schema, sql);
    Assert.assertNotNull(resultRoute);

    /**
     * 这些带有副作用的语句走主节点,即第一个dataNode
     */
    sql = "SELECT * FROM `travelrecord2` for update";
    dn1 = "dn1";
    resultRoute = router.enterRoute(schema, sql);
    Assert.assertEquals(new GlobalTableWriteResultRoute().setMaster(dn1).setSql(sql).setDataNodes(
        Arrays.asList("dn2", "dn3")), resultRoute);
  }

  @Test
  public void butAnyValue() {
    thrown.expect(MycatException.class);
    String sql = "select * from travelrecord;";
    String schema = "db1";
    String dn1 = "dn1";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce(dn1).setSqlOnce(sql));
  }

  @Test
  public void butSchema() {
    thrown.expect(MycatException.class);
    String sql = "SELECT * FROM `travelrecord` WHERE id = 0;";
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
  public void butDataNode() {
    thrown.expect(MycatException.class);
    String sql = "select 1;";
    String schema = "db1";
    String dn2 = "dn2";
    ResultRoute result = loadModule(module)
                             .enterRoute(schema, sql);
    Assert.assertEquals(result, new OneServerResultRoute().setDataNodeOnce("dn1").setSqlOnce(sql));
    Assert.assertNotEquals(result, new OneServerResultRoute().setDataNodeOnce(dn2).setSqlOnce(sql));
  }

  @Test
  public void multiSQL() {
    thrown.expect(MycatException.class);
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
