//package io.mycat.datasource.jdbc;
//
//import io.mycat.MycatException;
//import io.mycat.beans.mysql.MySQLAutoCommit;
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.beans.resultset.MycatUpdateResponse;
//import io.mycat.plug.loadBalance.LoadBalanceStrategy;
//import io.mycat.proxy.session.MycatSession;
//import javax.transaction.NotSupportedException;
//import javax.transaction.Status;
//import javax.transaction.SystemException;
//import javax.transaction.UserTransaction;
//
//public class JTADataNodeSession extends SimpleDataNodeSession {
//
//  private MycatSession session;
//  UserTransaction userTransaction;
//
//  public JTADataNodeSession(MycatSession session, GridRuntime jdbcRuntime) {
//    super(session, jdbcRuntime);
//    this.session = session;
//    this.userTransaction = null;
//  }
//
//  @Override
//  public void commit() {
//    inTranscation = false;
//    try {
//      userTransaction.commit();
//    } catch (Exception e) {
//      rollback();
//      throw new MycatException(e);
//    } finally {
//      close();
//    }
//  }
//
//  @Override
//  public void setAutomcommit(boolean on) {
//    this.autocommit = on ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF;
//    boolean inTranscation = this.inTranscation;
//    if (!on) {
//      this.inTranscation = true;
//      startTransaction();
//    }
//  }
//
//  @Override
//  public void startTransaction() {
//    inTranscation = true;
//    try {
//      //  userTransaction =null;
//      if (userTransaction == null) {
//        userTransaction = jdbcRuntime.getDatasourceProvider().createUserTransaction();
//      }
//      tryBegin();
//    } catch (Exception e) {
//      throw new MycatException(e);
//    }
//  }
//
//  private void tryBegin(){
//    try {
//      if ((inTranscation||autocommit==MySQLAutoCommit.OFF) && Status.STATUS_NO_TRANSACTION == userTransaction.getStatus()) {
//        userTransaction.begin();
//      }
//    } catch (SystemException e) {
//      e.printStackTrace();
//    } catch (NotSupportedException e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Override
//  public void rollback() {
//    inTranscation = false;
//    try {
//      userTransaction.setRollbackOnly();
//    } catch (SystemException e) {
//      throw new MycatException(e);
//    } finally {
//      // userTransaction =null;
//      close();
//    }
//  }
//
//  @Override
//  public MycatResultSetResponse executeQuery(MycatSession mycat, String dataNode, String sql,
//      boolean runOnMaster, LoadBalanceStrategy strategy) {
//    tryBegin();
//    return super.executeQuery(mycat, dataNode, sql, runOnMaster, strategy);
//  }
//
//  @Override
//  public MycatUpdateResponse executeUpdate(MycatSession mycat, String dataNode, String sql,
//      boolean insert, boolean runOnMaster, LoadBalanceStrategy strategy) {
//    tryBegin();
//    return super.executeUpdate(mycat, dataNode, sql, insert, runOnMaster, strategy);
//  }
//
//  @Override
//  public void onExceptionClose() {
//    //userTransaction = null;
//    super.onExceptionClose();
//  }
//
//  @Override
//  public void close() {
//    // userTransaction = null;
//    super.close();
//  }
//}