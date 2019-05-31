package io.mycat.test;

import java.sql.Connection;
import java.sql.SQLException;

public interface TestConnetionCallback {
  void test(Object future, Connection connection) throws SQLException, InterruptedException;
}