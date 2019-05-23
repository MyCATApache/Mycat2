package io.mycat.test.hibernate;

import io.mycat.test.pojo.TravelRecord;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

/**
 * @author jamie12221
 * @date 2019-05-24 01:27
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
