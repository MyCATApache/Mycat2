package io.mycat.datasource.jdbc.test;

import com.atomikos.icatch.jta.UserTransactionImp;
import javax.transaction.UserTransaction;

public class AtomikosTest {

  public static void main(String[] args) {
    UserTransaction userTransaction = new UserTransactionImp();
  }
}