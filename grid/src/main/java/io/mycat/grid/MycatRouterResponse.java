//package io.mycat.grid;
//
//import io.mycat.beans.mysql.MySQLFieldsType;
//import io.mycat.beans.resultset.MycatResponse;
//import io.mycat.beans.resultset.MycatResultSet;
//import io.mycat.proxy.ResultSetProvider;
//import io.mycat.proxy.session.MycatSession;
//import io.mycat.router.MycatRouter;
//import io.mycat.sqlparser.util.simpleParser.BufferSQLContext;
//
//import java.util.Collection;
//import java.util.Map.Entry;
//import java.util.Set;
//
//public class MycatRouterResponse {
//
//  public static MycatResponse showDb(MycatSession mycat, Collection<MycatSchema> schemaList) {
//    MycatResultSet resultSet = ResultSetProvider.INSTANCE
//        .createDefaultResultSet(1, mycat.charsetIndex(), mycat.charset());
//    resultSet.addColumnDef(0, "information_schema", "SCHEMATA", "SCHEMATA", "Database",
//        "SCHEMA_NAME",
//        MySQLFieldsType.FIELD_TYPE_VAR_STRING,
//        0x1, 0, 192);
//    for (MycatSchema schema : schemaList) {
//      resultSet.addTextRowPayload(schema.getSchemaName());
//    }
//    return resultSet;
//  }
//
//  public static MycatResponse showTable(MycatRouter router, MycatSession mycat, String schemaName) {
//    MycatRouterConfig config = router.getConfig();
//    MycatSchema schema = config.getSchemaOrDefaultBySchemaName(schemaName);
//    MycatResultSet resultSet = ResultSetProvider.INSTANCE
//        .createDefaultResultSet(2, mycat.charsetIndex(), mycat.charset());
//    resultSet.addColumnDef(0, "Tables in " + schemaName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//    resultSet.addColumnDef(1, "Table_type " + schemaName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//    for (String name : schema.getMycatTables().keySet()) {
//      resultSet.addTextRowPayload(name, "BASE TABLE");
//    }
//    return  resultSet;
//  }
//
//  public static MycatResponse showVariables(MycatSession mycat, Set<Entry<String, String>> entries ) {
//    MycatResultSet resultSet = ResultSetProvider.INSTANCE
//        .createDefaultResultSet(2, mycat.charsetIndex(), mycat.charset());
//    resultSet.addColumnDef(0, "Variable_name", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//    resultSet.addColumnDef(0, "Value", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//    for (Entry<String, String> entry : entries) {
//      resultSet.addTextRowPayload(entry.getKey(), entry.getValue());
//    }
//    return resultSet;
//  }
//
//  public static MycatResponse showWarnnings(MycatSession mycat) {
//    MycatResultSet resultSet = ResultSetProvider.INSTANCE
//        .createDefaultResultSet(3, mycat.charsetIndex(), mycat.charset());
//    mycat.writeColumnCount(3);
//    resultSet.addColumnDef(0, "Level", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//    resultSet.addColumnDef(1, "Code", MySQLFieldsType.FIELD_TYPE_LONG_BLOB);
//    resultSet.addColumnDef(2, "CMessage", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//    return  resultSet;
//  }
//
//  public static MycatResponse selectVariables(MycatSession mycat, BufferSQLContext sqlContext) {
//    MycatResultSet resultSet = ResultSetProvider.INSTANCE
//        .createDefaultResultSet(1, mycat.charsetIndex(), mycat.charset());
//    if (sqlContext.isSelectAutocommit()) {
//      resultSet.addColumnDef(0, "@@session.autocommit", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//      resultSet.addTextRowPayload(mycat.getAutoCommit().getText());
//      return null;
//    } else if (sqlContext.isSelectTxIsolation()) {
//      resultSet.addColumnDef(0, "@@session.tx_isolation", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
//      resultSet.addTextRowPayload(mycat.getIsolation().getText());
//      return null;
//    } else if (sqlContext.isSelectTranscationReadOnly()) {
//      resultSet.addColumnDef(0, "@@session.transaction_read_only",
//          MySQLFieldsType.FIELD_TYPE_LONGLONG);
//      resultSet.addTextRowPayload(mycat.getIsolation().getText());
//    }else {
//      return null;
//    }
//    return  resultSet;
//  }
//}