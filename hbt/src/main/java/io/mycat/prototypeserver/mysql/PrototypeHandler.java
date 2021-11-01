package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;

import java.util.List;

public interface PrototypeHandler {

    List<Object[]> showDataBase(com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement sqlShowDatabasesStatement);

    List<Object[]> showTables(SQLShowTablesStatement statement);

    List<Object[]> showColumns(SQLShowColumnsStatement statement);

    List<Object[]> showTableStatus(MySqlShowTableStatusStatement statement);

    List<Object[]> showCreateDatabase(MySqlShowCreateDatabaseStatement statement);

    List<Object[]> showCreateTable(SQLShowCreateTableStatement statement);

    List<Object[]> showCharacterSet(MySqlShowCharacterSetStatement statement);

    List<Object[]> showCollation(MySqlShowCollationStatement statement);

    List<Object[]> showStatus(MySqlShowStatusStatement statement);

    List<Object[]> showCreateFunction(MySqlShowCreateFunctionStatement statement);

    List<Object[]> showEngine(MySqlShowEnginesStatement statement);

    List<Object[]> showErrors(MySqlShowErrorsStatement statement);

    List<Object[]> showIndexesColumns(SQLShowIndexesStatement statement);

    List<Object[]> showProcedureStatus(MySqlShowProcedureStatusStatement statement);

    List<Object[]> showVariants(MySqlShowVariantsStatement statement);

    List<Object[]> showWarnings(MySqlShowWarningsStatement statement);
}
