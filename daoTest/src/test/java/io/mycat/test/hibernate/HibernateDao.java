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
package io.mycat.test.hibernate;

import io.mycat.MycatProxyBeanProviders;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import io.mycat.test.jdbc.TestGettingConnetionCallback;
import io.mycat.test.mybatis.DataConnection;
import io.mycat.test.pojo.TravelRecord;
import org.apache.ibatis.session.SqlSession;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import static io.mycat.test.ModualTest.compelete;
import static io.mycat.test.ModualTest.loadModule;

/**
 * @author jamie12221 date 2019-05-24 01:27
 **/
public class HibernateDao {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";
  public static DataConnection dataConn = new DataConnection();


  private static SessionFactory factory;

  @Test
  public void simplePass()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER,new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        new TestGettingConnetionCallback() {
          @Override
          public void test(Object future) throws IOException {
            try (SqlSession sqlSession = dataConn.getSqlSession()) {
              Configuration con = new Configuration();
              con.addAnnotatedClass(TravelRecord.class);
              con.configure("io/mycat/test/hibernate/hibernate.cfg.xml");
              factory = con.buildSessionFactory();
              try (Session currentSession = factory.getCurrentSession()) {
                Transaction transaction = currentSession.beginTransaction();
                currentSession.save(new TravelRecord());
                transaction.commit();
              }
              compelete(future);
            }
          }
        });
  }
}
