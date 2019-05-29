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

import io.mycat.test.pojo.TravelRecord;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

/**
 * @author jamie12221
 *  date 2019-05-24 01:27
 **/
public class HibernateDao {

  private static SessionFactory factory;

  public static void main(String[] args) {
    Configuration con = new Configuration();
    con.addAnnotatedClass(TravelRecord.class);
    con.configure("io/mycat/test/hibernate/hibernate.cfg.xml");
    factory = con.buildSessionFactory();
    try (Session currentSession = factory.getCurrentSession()) {
      Transaction transaction = currentSession.beginTransaction();
      currentSession.save(new TravelRecord());
      transaction.commit();
    }
  }
}
