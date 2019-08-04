package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.proxy.session.MycatSession;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

public class JTADataNodeSession extends SimpleDataNodeSession {

  private MycatSession session;
  UserTransaction userTransaction;

  public JTADataNodeSession(MycatSession session, GridRuntime jdbcRuntime) {
    super(session, jdbcRuntime);
    this.session = session;
    this.userTransaction = null;
  }

  @Override
  public void commit() {
    inTranscation = false;
    try {
      userTransaction.commit();
    } catch (Exception e) {
      rollback();
      throw new MycatException(e);
    } finally {
      userTransaction =null;
      close();
    }
  }

  @Override
  public void startTransaction() {
    inTranscation = true;
    try {
      userTransaction =null;
      userTransaction = jdbcRuntime.getDatasourceProvider().createUserTransaction();
      userTransaction.begin();
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  @Override
  public void rollback() {
    inTranscation = false;
    try {
      userTransaction.setRollbackOnly();
    } catch (SystemException e) {
      throw new MycatException(e);
    }finally {
      userTransaction =null;
      close();
    }
  }

}