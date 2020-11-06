package io.mycat;

public enum MycatSqlStatementType {
    //查询语句
    SQLSelectStatement,
    //插入语句
    MySqlInsertStatement,//  Insert,
    //
    SQLReplaceStatement,
    //DELETE
    MySqlDeleteStatement,//Delete,
    //update语句
    MySqlUpdateStatement,//Update,
    //LoadData
    MySqlLoadDataInFileStatement,// LoadData,
    SQLTruncateStatement,//Truncate,
    SQLSetStatement,
    MySqlSetTransactionStatement,
    SQLUseStatement,// Use

    //TCL
    //开启事务
    Begin,
    SQLCommitStatement,
    SQLRollbackStatement,
    SQLStartTransactionStatement,//StartTransaction,//SQLStartTransactionStatement
    MySqlKillStatement,// Kill,//


    //desc,describe,explain的别名
    MySqlExplainStatement,

    //DDL
    //mycat中存在的表添加字段,存储节点上的物理表添加索引
    SQLAlterTableAddColumn,
    //存储节点上的物理表添加索引
    SQLAlterTableAddIndex,
    //存储节点上的物理表修改字符集
    SQLAlterDatabaseStatement,
    //与mycat中的create语句结合生成新的create,并应用该结果,物理表应用该变化
    SQLAlterTableStatement,
    //创建逻辑库
    SQLCreateDatabaseStatement,
    //物理表创建索引
    SQLCreateIndexStatement,
    //创建逻辑表,该语句要带有存储节点信息
    SQLCreateTableStatement,
    //创建逻辑视图,暂时不支持,以后可能支持
    SQLCreateViewStatement,
    //逻辑表与物理表删除列
    SQLAlterTableDropColumnItem,
    //删除逻辑库
    SQLDropDatabaseStatement,
    //物理表删除索引
    SQLAlterTableDropIndex,
    //删除逻辑表
    SQLDropTableStatement,
    //删除逻辑视图
    SQLDropViewStatement,
    SQLAlterTableRenameIndex,
    MySqlAlterTableModifyColumn,
    MySqlRenameTableStatement,


    //show
    SQLShowTablesStatement,//show tables
    MySqlShowTableStatusStatement,//ShowTableStatus,
    MySqlShowVariantsStatement,//  ShowVariables,
    MySqlShowWarningsStatement,//ShowWarnings,
    MySqlShowProcessListStatement,
    SQLShowIndexesStatement,
    MySqlShowColumnsStatement,
    MySqlShowErrorsStatement,
    MySqlShowEnginesStatement,
    MySqlShowDatabaseStatusStatement,
    MySqlShowCreateTableStatement,
    MySqlShowCollationStatement,
    MySqlShowCharacterSetStatement,
}