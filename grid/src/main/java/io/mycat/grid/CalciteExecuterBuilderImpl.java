package io.mycat.grid;

import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.beans.resultset.SQLExecuter;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.proxy.ResultSetProvider;
import io.mycat.proxy.session.MycatSession;
import java.util.Arrays;

public class CalciteExecuterBuilderImpl implements ExecuterBuilder {

  private final MycatSession session;
  private final GRuntime jdbcRuntime;

  public CalciteExecuterBuilderImpl(MycatSession session, GRuntime jdbcRuntime) {

    this.session = session;
    this.jdbcRuntime = jdbcRuntime;
  }

  @Override
  public SQLExecuter[] generate(byte[] sqlBytes) {
    MycatResultSet resultSet = ResultSetProvider.INSTANCE
        .createDefaultResultSet(2, session.charsetIndex(), session.charset());
    resultSet.addColumnDef(0, "Tables in " + "haha", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    resultSet.addColumnDef(1, "Table_type " + "haha", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    for (String name : Arrays.asList("haha", "haha2")) {
      resultSet.addTextRowPayload(name, "BASE TABLE");
    }
    return new SQLExecuter[]{() -> resultSet};
  }
}