package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

public class JTADataNodeSession extends SimpleDataNodeSession {

  final UserTransaction userTransaction;

  public JTADataNodeSession(GridRuntime jdbcRuntime) {
    super(jdbcRuntime);
    this.userTransaction = jdbcRuntime.getDatasourceProvider().createUserTransaction();
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
      clear();
    }
  }

  @Override
  public void startTransaction() {
    inTranscation = true;
    try {
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
    }
  }

  @Override
  public void clear() {
    super.clear();
  }
}