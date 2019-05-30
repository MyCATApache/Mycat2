package io.mycat.test;

import java.sql.Connection;
import java.sql.SQLException;

public interface TestCallback {
  void test(Object future, Connection connection) throws SQLException;
}