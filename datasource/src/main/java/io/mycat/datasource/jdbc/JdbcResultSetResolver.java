package io.mycat.datasource.jdbc;

import io.mycat.datasource.jdbc.response.JDBCErrorResponse;
import io.mycat.datasource.jdbc.response.JDBCOkResponse;
import io.mycat.datasource.jdbc.response.JDBCResponse;
import io.mycat.datasource.jdbc.response.JDBCResultResponse;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcResultSetResolver {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JdbcResultSetResolver.class);

  public static JDBCResponse execute(Statement statement, String sql, boolean needGeneratedKeys) {
    try {
      if (needGeneratedKeys) {
        statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
        ResultSet generatedKeys = statement.getGeneratedKeys();
        long lastInsertId = generatedKeys.next() ? generatedKeys.getLong(0) : 0L;
        return new JDBCOkResponse(statement, statement.getUpdateCount(), lastInsertId);
      } else {
        return new JDBCResultResponse(statement, statement.executeQuery(sql));
      }
    } catch (SQLException e) {
      LOGGER.error("", e);
      return new JDBCErrorResponse(e);

    }

  }
}