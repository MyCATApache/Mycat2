package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;

import java.util.List;

public interface PrototypeHandler {
    List<Object[]> showDataBase(MySqlShowDatabaseStatusStatement mySqlShowDatabaseStatusStatement);

    List<Object[]> showTables(SQLShowTablesStatement statement);

    List<Object[]> showColumns(SQLShowColumnsStatement statement);

    List<Object[]> showTableStatus(MySqlShowTableStatusStatement statement);

    List<Object[]> showCreateDatabase(MySqlShowCreateDatabaseStatement statement);

    List<Object[]> showCreateTable(SQLShowCreateTableStatement statement);
}
