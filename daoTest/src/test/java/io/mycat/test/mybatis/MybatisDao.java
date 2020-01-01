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
package io.mycat.test.mybatis;

import io.mycat.MycatProxyBeanProviders;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import io.mycat.test.ModualTest;
import io.mycat.test.jdbc.TestGettingConnetionCallback;
import io.mycat.test.pojo.TravelRecord;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author jamie12221 date 2019-05-19 18:02
 **/
public class MybatisDao extends ModualTest {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";
  public static DataConnection dataConn = new DataConnection();

  @Test
  public void simplePass()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER,new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        new TestGettingConnetionCallback() {
          @Override
          public void test(Object future) throws IOException {
            try (SqlSession sqlSession = dataConn.getSqlSession()) {
              int i = 0;
              while (i < 10) {
                sqlSession.insert("test.insertTravelRecord", new TravelRecord());
                Object o = sqlSession.selectList("test.findTravelRecordById", 1);
                sqlSession.commit();
                ++i;
              }
            }
            compelete(future);
          }
        });

  }
}
