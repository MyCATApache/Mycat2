/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlparser.visitor;

import com.alibaba.druid.sql.ast.SQLArgument;
import com.alibaba.druid.sql.ast.SQLArrayDataType;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDeclareItem;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.ast.SQLKeep;
import com.alibaba.druid.sql.ast.SQLKeep.DenseRank;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLMapDataType;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.SQLOver.WindowingType;
import com.alibaba.druid.sql.ast.SQLParameter;
import com.alibaba.druid.sql.ast.SQLParameter.ParameterType;
import com.alibaba.druid.sql.ast.SQLPartition;
import com.alibaba.druid.sql.ast.SQLPartitionBy;
import com.alibaba.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.druid.sql.ast.SQLPartitionByList;
import com.alibaba.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.druid.sql.ast.SQLPartitionValue;
import com.alibaba.druid.sql.ast.SQLPartitionValue.Operator;
import com.alibaba.druid.sql.ast.SQLRecordDataType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.SQLStructDataType;
import com.alibaba.druid.sql.ast.SQLStructDataType.Field;
import com.alibaba.druid.sql.ast.SQLSubPartition;
import com.alibaba.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.druid.sql.ast.SQLSubPartitionByList;
import com.alibaba.druid.sql.ast.SQLWindow;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.druid.sql.ast.expr.SQLArrayExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseStatement;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLContainsExpr;
import com.alibaba.druid.sql.ast.expr.SQLCurrentOfCursorExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLFlashbackExpr;
import com.alibaba.druid.sql.ast.expr.SQLGroupingSetExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLRealExpr;
import com.alibaba.druid.sql.ast.expr.SQLSequenceExpr;
import com.alibaba.druid.sql.ast.expr.SQLSequenceExpr.Function;
import com.alibaba.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLValuesExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterCharacter;
import com.alibaba.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterSequenceStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAlterColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAnalyzePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableCheckPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableCoalescePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableConvertCharSet;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDisableConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDisableKeys;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDisableLifecycle;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDiscardPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropForeignKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableEnableConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableEnableKeys;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableEnableLifecycle;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableExchangePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableImportPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableOptimizePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableReOrganizePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRebuildPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRenamePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRepairPartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableSetComment;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableSetLifecycle;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableTouch;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableTruncatePartition;
import com.alibaba.druid.sql.ast.statement.SQLAlterTypeStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterViewRenameStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLCheck;
import com.alibaba.druid.sql.ast.statement.SQLCloseStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnCheck;
import com.alibaba.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition.Identity;
import com.alibaba.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.druid.sql.ast.statement.SQLCommentStatement;
import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateMaterializedViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTriggerStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTriggerStatement.TriggerType;
import com.alibaba.druid.sql.ast.statement.SQLCreateUserStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement.Column;
import com.alibaba.druid.sql.ast.statement.SQLDeclareStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLDescribeStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropEventStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropFunctionStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropLogFileGroupStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropMaterializedViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropServerStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropSynonymStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableSpaceStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTriggerStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTypeStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLDumpStatement;
import com.alibaba.druid.sql.ast.statement.SQLErrorLoggingClause;
import com.alibaba.druid.sql.ast.statement.SQLExplainStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprHint;
import com.alibaba.druid.sql.ast.statement.SQLExprStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLExternalRecordFormat;
import com.alibaba.druid.sql.ast.statement.SQLFetchStatement;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyImpl;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyImpl.Match;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyImpl.Option;
import com.alibaba.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.druid.sql.ast.statement.SQLIfStatement.Else;
import com.alibaba.druid.sql.ast.statement.SQLIfStatement.ElseIf;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.druid.sql.ast.statement.SQLMergeStatement;
import com.alibaba.druid.sql.ast.statement.SQLMergeStatement.MergeInsertClause;
import com.alibaba.druid.sql.ast.statement.SQLMergeStatement.MergeUpdateClause;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLNullConstraint;
import com.alibaba.druid.sql.ast.statement.SQLObjectType;
import com.alibaba.druid.sql.ast.statement.SQLOpenStatement;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKeyImpl;
import com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.druid.sql.ast.statement.SQLReturnStatement;
import com.alibaba.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLScriptCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem.NullsOrderType;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowErrorsStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnique;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause.Entry;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.ConditionValue;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement.MySqlWhenStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlDeclareConditionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlDeclareHandlerStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlHandlerType;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlLeaveStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlSelectIntoStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr.SearchModifier;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.CobarShowStatus;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterEventStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterLogFileGroupStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterServerStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableDiscardTablespace;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableImportTablespace;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTablespaceStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlChecksumTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateAddLogFileGroupStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateEventStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateServerStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableSpaceStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlEventSchedule;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHelpStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement.Type;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement.LockType;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlOptimizeStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement.Item;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlResetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateViewStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasePartitionStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProfilesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByList;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.druid.sql.repository.SchemaObject;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-06 13:54
 **/
public class MyStatementVisitor implements MySqlASTVisitor {

  @Override
  public boolean visit(MySqlTableIndex x) {
    SQLName name = x.getName();
    String indexType = x.getIndexType();
    List<SQLSelectOrderByItem> columns = x.getColumns();

    return false;
  }

  @Override
  public void endVisit(MySqlTableIndex x) {
    SQLName name = x.getName();
    String indexType = x.getIndexType();
    List<SQLSelectOrderByItem> columns = x.getColumns();
  }

  @Override
  public boolean visit(MySqlKey x) {
    String indexType = x.getIndexType();
    boolean hasConstaint = x.isHasConstaint();
    SQLExpr keyBlockSize = x.getKeyBlockSize();
    return false;
  }

  @Override
  public void endVisit(MySqlKey x) {
    String indexType = x.getIndexType();

    boolean hasConstaint = x.isHasConstaint();

    SQLExpr keyBlockSize = x.getKeyBlockSize();
  }

  @Override
  public boolean visit(MySqlPrimaryKey x) {
    return false;
  }

  @Override
  public void endVisit(MySqlPrimaryKey x) {

  }

  @Override
  public boolean visit(MySqlUnique x) {
    return false;
  }

  @Override
  public void endVisit(MySqlUnique x) {

  }

  @Override
  public boolean visit(MysqlForeignKey x) {
    return false;
  }

  @Override
  public void endVisit(MysqlForeignKey x) {
    SQLName indexName = x.getIndexName();
    boolean hasConstraint = x.isHasConstraint();
    Match referenceMatch = x.getReferenceMatch();
    Option onUpdate = x.getOnUpdate();
    Option onDelete = x.getOnDelete();
  }

  @Override
  public void endVisit(MySqlExtractExpr x) {
    SQLExpr value = x.getValue();
    SQLIntervalUnit unit = x.getUnit();

  }

  @Override
  public boolean visit(MySqlExtractExpr x) {
    SQLExpr value = x.getValue();
    SQLIntervalUnit unit = x.getUnit();
    return false;
  }

  @Override
  public void endVisit(MySqlMatchAgainstExpr x) {
    List<SQLExpr> columns = x.getColumns();

    SQLExpr against = x.getAgainst();

    SearchModifier searchModifier = x.getSearchModifier();

  }

  @Override
  public boolean visit(MySqlMatchAgainstExpr x) {
    List<SQLExpr> columns = x.getColumns();

    SQLExpr against = x.getAgainst();

    SearchModifier searchModifier = x.getSearchModifier();
    return false;
  }

  @Override
  public void endVisit(MySqlPrepareStatement x) {
    SQLName name = x.getName();
    SQLExpr from = x.getFrom();

  }

  @Override
  public boolean visit(MySqlPrepareStatement x) {
    SQLName name = x.getName();
    SQLExpr from = x.getFrom();
    return false;
  }

  @Override
  public void endVisit(MySqlExecuteStatement x) {
    SQLName statementName = x.getStatementName();
    final List<SQLExpr> parameters = x.getParameters();

  }

  @Override
  public boolean visit(MysqlDeallocatePrepareStatement x) {
    SQLName statementName = x.getStatementName();
    return false;
  }

  @Override
  public void endVisit(MysqlDeallocatePrepareStatement x) {
    SQLName statementName = x.getStatementName();
  }

  @Override
  public boolean visit(MySqlExecuteStatement x) {
    SQLName statementName = x.getStatementName();
    final List<SQLExpr> parameters = x.getParameters();
    return false;
  }

  @Override
  public void endVisit(MySqlDeleteStatement x) {
    boolean lowPriority = x.isLowPriority();
    boolean quick = x.isQuick();
    boolean ignore = x.isIgnore();
    SQLOrderBy orderBy = x.getOrderBy();
    SQLLimit limit = x.getLimit();
    List<SQLCommentHint> hints = x.getHints();
    // for petadata
    boolean forceAllPartitions = x.isForceAllPartitions();
    SQLName forcePartition = x.getForcePartition();

  }

  @Override
  public boolean visit(MySqlDeleteStatement x) {
    boolean lowPriority = x.isLowPriority();
    boolean quick = x.isQuick();
    boolean ignore = x.isIgnore();
    SQLOrderBy orderBy = x.getOrderBy();
    SQLLimit limit = x.getLimit();
    List<SQLCommentHint> hints = x.getHints();
    // for petadata
    boolean forceAllPartitions = x.isForceAllPartitions();
    SQLName forcePartition = x.getForcePartition();
    return false;
  }

  @Override
  public void endVisit(MySqlInsertStatement x) {
    boolean lowPriority = x.isLowPriority();
    boolean delayed = x.isDelayed();
    boolean highPriority = x.isHighPriority();
    boolean ignore = x.isIgnore();
    boolean rollbackOnFail = x.isRollbackOnFail();

    final List<SQLExpr> duplicateKeyUpdate = x.getDuplicateKeyUpdate();
  }

  @Override
  public boolean visit(MySqlInsertStatement x) {
    boolean lowPriority = x.isLowPriority();
    boolean delayed = x.isDelayed();
    boolean highPriority = x.isHighPriority();
    boolean ignore = x.isIgnore();
    boolean rollbackOnFail = x.isRollbackOnFail();

    final List<SQLExpr> duplicateKeyUpdate = x.getDuplicateKeyUpdate();
    return false;
  }

  @Override
  public void endVisit(MySqlLoadDataInFileStatement x) {
    boolean lowPriority = x.isLowPriority();
    boolean concurrent = x.isConcurrent();
    boolean local = x.isLocal();

    SQLLiteralExpr fileName = x.getFileName();

    boolean replicate = x.isReplicate();
    boolean ignore = false;

    SQLName tableName = x.getTableName();

    String charset = x.getCharset();

    SQLLiteralExpr columnsTerminatedBy = x.getColumnsTerminatedBy();
    boolean columnsEnclosedOptionally = x.isColumnsEnclosedOptionally();
    SQLLiteralExpr columnsEnclosedBy = x.getColumnsEnclosedBy();
    SQLLiteralExpr columnsEscaped = x.getColumnsEscaped();

    SQLLiteralExpr linesStartingBy = x.getLinesStartingBy();
    SQLLiteralExpr linesTerminatedBy = x.getLinesTerminatedBy();

    SQLExpr ignoreLinesNumber = x.getIgnoreLinesNumber();

    List<SQLExpr> setList = x.getSetList();

    List<SQLExpr> columns = x.getColumns();
  }

  @Override
  public boolean visit(MySqlLoadDataInFileStatement x) {
    boolean lowPriority = x.isLowPriority();
    boolean concurrent = x.isConcurrent();
    boolean local = x.isLocal();

    SQLLiteralExpr fileName = x.getFileName();

    boolean replicate = x.isReplicate();
    boolean ignore = false;

    SQLName tableName = x.getTableName();

    String charset = x.getCharset();

    SQLLiteralExpr columnsTerminatedBy = x.getColumnsTerminatedBy();
    boolean columnsEnclosedOptionally = x.isColumnsEnclosedOptionally();
    SQLLiteralExpr columnsEnclosedBy = x.getColumnsEnclosedBy();
    SQLLiteralExpr columnsEscaped = x.getColumnsEscaped();

    SQLLiteralExpr linesStartingBy = x.getLinesStartingBy();
    SQLLiteralExpr linesTerminatedBy = x.getLinesTerminatedBy();

    SQLExpr ignoreLinesNumber = x.getIgnoreLinesNumber();

    List<SQLExpr> setList = x.getSetList();

    List<SQLExpr> columns = x.getColumns();

    return false;
  }

  @Override
  public void endVisit(MySqlLoadXmlStatement x) {
     boolean lowPriority = x.isLowPriority();
     boolean concurrent = x.isConcurrent();
     boolean local = x.isLocal();

     SQLLiteralExpr fileName = x.getFileName();

     boolean replicate = x.isReplicate();
     boolean ignore = x.isIgnore();

     SQLName tableName = x.getTableName();

     String charset = x.getCharset();

     SQLExpr rowsIdentifiedBy = x.getRowsIdentifiedBy();

     SQLExpr ignoreLinesNumber = x.getIgnoreLinesNumber();

     final List<SQLExpr> setList = x.getSetList();
  }

  @Override
  public boolean visit(MySqlLoadXmlStatement x) {
     boolean lowPriority = x.isLowPriority();
     boolean concurrent = x.isConcurrent();
     boolean local = x.isLocal();

     SQLLiteralExpr fileName = x.getFileName();

     boolean replicate = x.isReplicate();
     boolean ignore = x.isIgnore();

     SQLName tableName = x.getTableName();

     String charset = x.getCharset();

     SQLExpr rowsIdentifiedBy = x.getRowsIdentifiedBy();

     SQLExpr ignoreLinesNumber = x.getIgnoreLinesNumber();

     final List<SQLExpr> setList = x.getSetList();
    return false;
  }

  @Override
  public void endVisit(MySqlShowColumnsStatement x) {
     boolean full = x.isFull();

     SQLName table = x.getTable();
     SQLName database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();

  }

  @Override
  public boolean visit(MySqlShowColumnsStatement x) {
     boolean full = x.isFull();

     SQLName table = x.getTable();
     SQLName database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowDatabasesStatement x) {
     SQLName database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowDatabasesStatement x) {
     SQLName database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowWarningsStatement x) {
     boolean count = x.isCount();
     SQLLimit limit = x.getLimit();


  }

  @Override
  public boolean visit(MySqlShowWarningsStatement x) {
     boolean count = x.isCount();
     SQLLimit limit = x.getLimit();
    return false;
  }

  @Override
  public void endVisit(MySqlShowStatusStatement x) {

     boolean global = x.isGlobal();
     boolean session = x.isSession();

     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowStatusStatement x) {
     boolean global = x.isGlobal();
     boolean session = x.isSession();

     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowAuthorsStatement x) {
//null
  }

  @Override
  public boolean visit(MySqlShowAuthorsStatement x) {
    // null
    return false;
  }

  @Override
  public void endVisit(CobarShowStatus x) {
    // null
  }

  @Override
  public boolean visit(CobarShowStatus x) {
    // null
    return false;
  }

  @Override
  public void endVisit(MySqlKillStatement x) {
     Type type = x.getType();
     List<SQLExpr> threadIds = x.getThreadIds();
  }

  @Override
  public boolean visit(MySqlKillStatement x) {
     Type type = x.getType();
     List<SQLExpr> threadIds = x.getThreadIds();
    return false;
  }

  @Override
  public void endVisit(MySqlBinlogStatement x) {
     SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(MySqlBinlogStatement x) {
     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(MySqlResetStatement x) {
     List<String> options = x.getOptions();
  }

  @Override
  public boolean visit(MySqlResetStatement x) {
     List<String> options = x
                                       .getOptions();
    return false;
  }

  @Override
  public void endVisit(MySqlCreateUserStatement x) {

     List<UserSpecification> users = x
                                                .getUsers();
  }

  @Override
  public boolean visit(MySqlCreateUserStatement x) {

     List<UserSpecification> users = x
                                                .getUsers();
    return false;
  }

  @Override
  public void endVisit(UserSpecification x) {
     SQLExpr user = x.getUser();
     boolean passwordHash = x.isPasswordHash();
     SQLExpr password = x.getPassword();
     SQLExpr authPlugin = x.getAuthPlugin();
  }

  @Override
  public boolean visit(UserSpecification x) {
     SQLExpr user = x.getUser();
     boolean passwordHash = x.isPasswordHash();
     SQLExpr password = x.getPassword();
     SQLExpr authPlugin = x.getAuthPlugin();
    return false;
  }

  @Override
  public void endVisit(MySqlPartitionByKey x) {
    short algorithm = x.getAlgorithm();
  }

  @Override
  public boolean visit(MySqlPartitionByKey x) {
    short algorithm = x.getAlgorithm();
    return false;
  }

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
     boolean hignPriority = x.isHignPriority();
     boolean straightJoin = x.isStraightJoin();
     boolean smallResult = x.isSmallResult();
     boolean bigResult = x.isBigResult();
     boolean bufferResult = x.isBufferResult();
     Boolean cache = x.getCache();
     boolean calcFoundRows = x.isCalcFoundRows();
     SQLName procedureName = x.getProcedureName();
     List<SQLExpr> procedureArgumentList = x.getProcedureArgumentList();
     boolean lockInShareMode = x.isLockInShareMode();
     SQLName forcePartition = x.getForcePartition(); // for petadata
    return false;
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
     boolean hignPriority = x.isHignPriority();
     boolean straightJoin = x.isStraightJoin();
     boolean smallResult = x.isSmallResult();
     boolean bigResult = x.isBigResult();
     boolean bufferResult = x.isBufferResult();
     Boolean cache = x.getCache();
     boolean calcFoundRows = x.isCalcFoundRows();
     SQLName procedureName = x.getProcedureName();
     List<SQLExpr> procedureArgumentList = x.getProcedureArgumentList();
     boolean lockInShareMode = x.isLockInShareMode();
     SQLName forcePartition = x.getForcePartition(); // for petadata
  }

  @Override
  public boolean visit(MySqlOutFileExpr x) {
     SQLExpr file = x.getFile();
     String charset = x.getCharset();

     SQLExpr columnsTerminatedBy = x.getColumnsTerminatedBy();
     boolean columnsEnclosedOptionally = x.isColumnsEnclosedOptionally();
     SQLLiteralExpr columnsEnclosedBy = x.getColumnsEnclosedBy();
     SQLLiteralExpr columnsEscaped = x.getColumnsEscaped();

     SQLLiteralExpr linesStartingBy = x.getLinesStartingBy();
     SQLLiteralExpr linesTerminatedBy = x.getLinesTerminatedBy();

     SQLExpr ignoreLinesNumber = x.getIgnoreLinesNumber();
    return false;
  }

  @Override
  public void endVisit(MySqlOutFileExpr x) {
     SQLExpr file = x.getFile();
     String charset = x.getCharset();

     SQLExpr columnsTerminatedBy = x.getColumnsTerminatedBy();
     boolean columnsEnclosedOptionally = x.isColumnsEnclosedOptionally();
     SQLLiteralExpr columnsEnclosedBy = x.getColumnsEnclosedBy();
     SQLLiteralExpr columnsEscaped = x.getColumnsEscaped();

     SQLLiteralExpr linesStartingBy = x.getLinesStartingBy();
     SQLLiteralExpr linesTerminatedBy = x.getLinesTerminatedBy();

     SQLExpr ignoreLinesNumber = x.getIgnoreLinesNumber();
  }

  @Override
  public boolean visit(MySqlExplainStatement x) {
     boolean describe = x.isDescribe();
     SQLName tableName = x.getTableName();
     SQLName columnName = x.getColumnName();
     SQLExpr wild = x.getWild();
     String format = x.getFormat();
     SQLExpr connectionId = x.getConnectionId();
    return false;
  }

  @Override
  public void endVisit(MySqlExplainStatement x) {
     boolean describe = x.isDescribe();
     SQLName tableName = x.getTableName();
     SQLName columnName = x.getColumnName();
     SQLExpr wild = x.getWild();
     String format = x.getFormat();
     SQLExpr connectionId = x.getConnectionId();
  }

  @Override
  public boolean visit(MySqlUpdateStatement x) {
     SQLLimit limit = x.getLimit();

     boolean lowPriority = x.isLowPriority();
     boolean ignore = x.isIgnore();
     boolean commitOnSuccess = x.isCommitOnSuccess();
     boolean rollBackOnFail = x.isRollBackOnFail();
     boolean queryOnPk = x.isQueryOnPk();
     SQLExpr targetAffectRow = x.getTargetAffectRow();

    // for petadata
     boolean forceAllPartitions = x
                                             .isForceAllPartitions();
     SQLName forcePartition = x.getForcePartition();
    return false;
  }

  @Override
  public void endVisit(MySqlUpdateStatement x) {
     SQLLimit limit = x.getLimit();

     boolean lowPriority = x.isLowPriority();
     boolean ignore = x.isIgnore();
     boolean commitOnSuccess = x.isCommitOnSuccess();
     boolean rollBackOnFail = x.isRollBackOnFail();
     boolean queryOnPk = x.isQueryOnPk();
     SQLExpr targetAffectRow = x.getTargetAffectRow();

    // for petadata
     boolean forceAllPartitions = x
                                             .isForceAllPartitions();
     SQLName forcePartition = x.getForcePartition();
  }

  @Override
  public boolean visit(MySqlSetTransactionStatement x) {
     Boolean global = x.getGlobal();

     String isolationLevel = x.getIsolationLevel();

     String accessModel = x.getAccessModel();

     Boolean session = x.getSession();
    return false;
  }

  @Override
  public void endVisit(MySqlSetTransactionStatement x) {
     Boolean global = x.getGlobal();

     String isolationLevel = x.getIsolationLevel();

     String accessModel = x.getAccessModel();

     Boolean session = x.getSession();
  }

  @Override
  public boolean visit(MySqlShowBinaryLogsStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(MySqlShowBinaryLogsStatement x) {
    //null

  }

  @Override
  public boolean visit(MySqlShowMasterLogsStatement x) {
    //null

    return false;
  }

  @Override
  public void endVisit(MySqlShowMasterLogsStatement x) {
    //null

  }

  @Override
  public boolean visit(MySqlShowCharacterSetStatement x) {
    return false;
  }

  @Override
  public void endVisit(MySqlShowCharacterSetStatement x) {
     SQLExpr where = x.getWhere();
     SQLExpr pattern = x.getPattern();
  }

  @Override
  public boolean visit(MySqlShowCollationStatement x) {
     SQLExpr where = x.getWhere();
     SQLExpr pattern = x.getPattern();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCollationStatement x) {
     SQLExpr where = x.getWhere();
     SQLExpr pattern = x.getPattern();
  }

  @Override
  public boolean visit(MySqlShowBinLogEventsStatement x) {
     SQLExpr in = x.getIn();
     SQLExpr from = x.getFrom();
     SQLLimit limit = x.getLimit();
    return false;
  }

  @Override
  public void endVisit(MySqlShowBinLogEventsStatement x) {
     SQLExpr in = x.getIn();
     SQLExpr from = x.getFrom();
     SQLLimit limit = x.getLimit();
  }

  @Override
  public boolean visit(MySqlShowContributorsStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(MySqlShowContributorsStatement x) {
    //null

  }

  @Override
  public boolean visit(MySqlShowCreateDatabaseStatement x) {
     SQLExpr database = x.getDatabase();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateDatabaseStatement x) {
     SQLExpr database = x.getDatabase();
  }

  @Override
  public boolean visit(MySqlShowCreateEventStatement x) {
     SQLExpr eventName = x.getEventName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateEventStatement x) {
     SQLExpr eventName = x.getEventName();
  }

  @Override
  public boolean visit(MySqlShowCreateFunctionStatement x) {
     SQLExpr name = x.getName();

    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateFunctionStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowCreateProcedureStatement x) {
     SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateProcedureStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowCreateTableStatement x) {
     SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateTableStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowCreateTriggerStatement x) {
     SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateTriggerStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowCreateViewStatement x) {
     SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowCreateViewStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowEngineStatement x) {

     SQLExpr name = x.getName();
     MySqlShowEngineStatement.Option option = x.getOption();
    return false;
  }

  @Override
  public void endVisit(MySqlShowEngineStatement x) {

     SQLExpr name = x.getName();
     MySqlShowEngineStatement.Option option = x.getOption();
  }

  @Override
  public boolean visit(MySqlShowEnginesStatement x) {
    boolean storage = x.isStorage();
    return false;
  }

  @Override
  public void endVisit(MySqlShowEnginesStatement x) {
    boolean storage = x.isStorage();
  }

  @Override
  public boolean visit(MySqlShowErrorsStatement x) {
     boolean count = x.isCount();
     SQLLimit limit = x.getLimit();
    return false;
  }

  @Override
  public void endVisit(MySqlShowErrorsStatement x) {
     boolean count = x.isCount();
     SQLLimit limit = x.getLimit();
  }

  @Override
  public boolean visit(MySqlShowEventsStatement x) {
     SQLExpr schema = x.getSchema();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowEventsStatement x) {
     SQLExpr schema = x.getSchema();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowFunctionCodeStatement x) {
     SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowFunctionCodeStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowFunctionStatusStatement x) {
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowFunctionStatusStatement x) {
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowGrantsStatement x) {
     SQLExpr user = x.getUser();
    return false;
  }

  @Override
  public void endVisit(MySqlShowGrantsStatement x) {
     SQLExpr user = x.getUser();
  }

  @Override
  public boolean visit(MySqlUserName x) {
     String userName = x.getUserName();
     String host = x.getHost();
     String identifiedBy = x.getIdentifiedBy();

    return false;
  }

  @Override
  public void endVisit(MySqlUserName x) {
     String userName = x.getUserName();
     String host = x.getHost();
     String identifiedBy = x.getIdentifiedBy();
  }

  @Override
  public boolean visit(MySqlShowIndexesStatement x) {

     SQLName table = x.getTable();
     SQLName database = x.getDatabase();
     List<SQLCommentHint> hints = x.getHints();

    return false;
  }

  @Override
  public void endVisit(MySqlShowIndexesStatement x) {
     SQLName table = x.getTable();
     SQLName database = x.getDatabase();
     List<SQLCommentHint> hints = x.getHints();
  }

  @Override
  public boolean visit(MySqlShowKeysStatement x) {
     SQLName table = x.getTable();
     SQLName database = x.getDatabase();
    return false;
  }

  @Override
  public void endVisit(MySqlShowKeysStatement x) {
     SQLName table = x.getTable();
     SQLName database = x.getDatabase();
  }

  @Override
  public boolean visit(MySqlShowMasterStatusStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(MySqlShowMasterStatusStatement x) {
//null
  }

  @Override
  public boolean visit(MySqlShowOpenTablesStatement x) {

     SQLExpr database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowOpenTablesStatement x) {
     SQLExpr database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowPluginsStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(MySqlShowPluginsStatement x) {
    //null
  }

  @Override
  public boolean visit(MySqlShowPrivilegesStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(MySqlShowPrivilegesStatement x) {
    //null
  }

  @Override
  public boolean visit(MySqlShowProcedureCodeStatement x) {
    SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(MySqlShowProcedureCodeStatement x) {

     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(MySqlShowProcedureStatusStatement x) {
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowProcedureStatusStatement x) {

     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowProcessListStatement x) {

     boolean full = x.isFull();
     SQLExpr where = x.getWhere();

    return false;
  }

  @Override
  public void endVisit(MySqlShowProcessListStatement x) {
     boolean full = x.isFull();
     SQLExpr where = x.getWhere();

  }

  @Override
  public boolean visit(MySqlShowProfileStatement x) {

     List<MySqlShowProfileStatement.Type> types = x.getTypes();

     SQLExpr forQuery = x
                                   .getForQuery();

     SQLLimit limit = x.getLimit();
    return false;
  }

  @Override
  public void endVisit(MySqlShowProfileStatement x) {

     List<MySqlShowProfileStatement.Type> types = x.getTypes();

     SQLExpr forQuery = x
                                   .getForQuery();

     SQLLimit limit = x.getLimit();
  }

  @Override
  public boolean visit(MySqlShowProfilesStatement x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlShowProfilesStatement x) {
    //null

  }

  @Override
  public boolean visit(MySqlShowRelayLogEventsStatement x) {

     SQLExpr logName = x.getLogName();
     SQLExpr from = x.getFrom();
     SQLLimit limit = x.getLimit();
    return false;
  }

  @Override
  public void endVisit(MySqlShowRelayLogEventsStatement x) {
     SQLExpr logName = x.getLogName();
     SQLExpr from = x.getFrom();
     SQLLimit limit = x.getLimit();
  }

  @Override
  public boolean visit(MySqlShowSlaveHostsStatement x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlShowSlaveHostsStatement x) {//null

  }

  @Override
  public boolean visit(MySqlShowSlaveStatusStatement x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlShowSlaveStatusStatement x) {//null

  }

  @Override
  public boolean visit(MySqlShowTableStatusStatement x) {

     SQLExpr database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowTableStatusStatement x) {
     SQLExpr database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowTriggersStatement x) {
     SQLExpr database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowTriggersStatement x) {
     SQLExpr database = x.getDatabase();
     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(MySqlShowVariantsStatement x) {

     boolean global = x.isGlobal();
     boolean session = x.isSession();

     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MySqlShowVariantsStatement x) {
     boolean global = x.isGlobal();
     boolean session = x.isSession();

     SQLExpr like = x.getLike();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(Item x) {
     SQLName name = x.getName();
     SQLName to = x.getTo();
    return false;
  }

  @Override
  public void endVisit(Item x) {
     SQLName name = x.getName();
     SQLName to = x.getTo();
  }

  @Override
  public boolean visit(MySqlRenameTableStatement x) {

     List<Item> items = x.getItems();
    return false;
  }

  @Override
  public void endVisit(MySqlRenameTableStatement x) {
     List<Item> items = x.getItems();
  }

  @Override
  public boolean visit(MySqlUseIndexHint x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlUseIndexHint x) {//null

  }

  @Override
  public boolean visit(MySqlIgnoreIndexHint x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlIgnoreIndexHint x) {//null

  }

  @Override
  public boolean visit(MySqlLockTableStatement x) {

     List<MySqlLockTableStatement.Item> items = x.getItems();
    return false;
  }

  @Override
  public void endVisit(MySqlLockTableStatement x) {
     List<MySqlLockTableStatement.Item> items = x.getItems();
  }

  @Override
  public boolean visit(MySqlLockTableStatement.Item x) {
     SQLExprTableSource tableSource = x.getTableSource();

     LockType lockType = x.getLockType();

     List<SQLCommentHint> hints = x.getHints();
    return false;
  }

  @Override
  public void endVisit(MySqlLockTableStatement.Item x) {
     SQLExprTableSource tableSource = x.getTableSource();

     LockType lockType = x.getLockType();

     List<SQLCommentHint> hints = x.getHints();
  }

  @Override
  public boolean visit(MySqlUnlockTablesStatement x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlUnlockTablesStatement x) {//null

  }

  @Override
  public boolean visit(MySqlForceIndexHint x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlForceIndexHint x) {//null

  }

  @Override
  public boolean visit(MySqlAlterTableChangeColumn x) {

     SQLName columnName = x.getColumnName();

     SQLColumnDefinition newColumnDefinition = x.getNewColumnDefinition();

     boolean first = x.isFirst();

     SQLName firstColumn = x.getFirstColumn();
     SQLName afterColumn = x.getAfterColumn();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterTableChangeColumn x) {

     SQLName columnName = x.getColumnName();

     SQLColumnDefinition newColumnDefinition = x.getNewColumnDefinition();

     boolean first = x.isFirst();

     SQLName firstColumn = x.getFirstColumn();
     SQLName afterColumn = x.getAfterColumn();
  }

  @Override
  public boolean visit(MySqlAlterTableOption x) {

     String name = x.getName();
     SQLObject value = x.getValue();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterTableOption x) {

     String name = x.getName();
     SQLObject value = x.getValue();
  }

  @Override
  public boolean visit(MySqlCreateTableStatement x) {
     Map<String, SQLObject> tableOptions = x.getTableOptions();
     List<SQLCommentHint> hints = x.getHints();
     List<SQLCommentHint> optionHints = x.getOptionHints();
     SQLName tableGroup = x.getTableGroup();

     SQLPartitionBy dbPartitionBy = x.getDbPartitionBy();
     SQLPartitionBy tablePartitionBy = x.getTablePartitionBy();
     SQLExpr tbpartitions = x.getTbpartitions();
    return false;
  }

  @Override
  public void endVisit(MySqlCreateTableStatement x) {
     Map<String, SQLObject> tableOptions = x.getTableOptions();
     List<SQLCommentHint> hints = x.getHints();
     List<SQLCommentHint> optionHints = x.getOptionHints();
     SQLName tableGroup = x.getTableGroup();

     SQLPartitionBy dbPartitionBy = x.getDbPartitionBy();
     SQLPartitionBy tablePartitionBy = x.getTablePartitionBy();
     SQLExpr tbpartitions = x.getTbpartitions();
  }

  @Override
  public boolean visit(MySqlHelpStatement x) {
     SQLExpr content = x.getContent();
    return false;
  }

  @Override
  public void endVisit(MySqlHelpStatement x) {
     SQLExpr content = x.getContent();

  }

  @Override
  public boolean visit(MySqlCharExpr x) {

     String charset = x.getCharset();
     String collate = x.getCollate();
    return false;
  }

  @Override
  public void endVisit(MySqlCharExpr x) {

     String charset = x.getCharset();
     String collate = x.getCollate();
  }

  @Override
  public boolean visit(MySqlAlterTableModifyColumn x) {

     SQLColumnDefinition newColumnDefinition = x.getNewColumnDefinition();

     boolean first = x.isFirst();

     SQLName firstColumn = x.getFirstColumn();
     SQLName afterColumn = x.getAfterColumn();

    return false;
  }

  @Override
  public void endVisit(MySqlAlterTableModifyColumn x) {
     SQLColumnDefinition newColumnDefinition = x.getNewColumnDefinition();

     boolean first = x.isFirst();

     SQLName firstColumn = x.getFirstColumn();
     SQLName afterColumn = x.getAfterColumn();
  }

  @Override
  public boolean visit(MySqlAlterTableDiscardTablespace x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlAlterTableDiscardTablespace x) {//null

  }

  @Override
  public boolean visit(MySqlAlterTableImportTablespace x) {//null
    return false;
  }

  @Override
  public void endVisit(MySqlAlterTableImportTablespace x) {//null

  }

  @Override
  public boolean visit(TableSpaceOption x) {
    SQLName name = x.getName();
    SQLExpr storage = x.getStorage();
    return false;
  }

  @Override
  public void endVisit(TableSpaceOption x) {
    SQLName name = x.getName();
    SQLExpr storage = x.getStorage();
  }

  @Override
  public boolean visit(MySqlAnalyzeStatement x) {
     boolean noWriteToBinlog = x.isNoWriteToBinlog();
     boolean local = x.isLocal();

     final List<SQLExprTableSource> tableSources = x.getTableSources();
    return false;
  }

  @Override
  public void endVisit(MySqlAnalyzeStatement x) {
     boolean noWriteToBinlog = x.isNoWriteToBinlog();
     boolean local = x.isLocal();

     final List<SQLExprTableSource> tableSources = x.getTableSources();
  }

  @Override
  public boolean visit(MySqlAlterUserStatement x) {

     final List<SQLExpr> users = x.getUsers();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterUserStatement x) {
     final List<SQLExpr> users = x.getUsers();
  }

  @Override
  public boolean visit(MySqlOptimizeStatement x) {
     boolean noWriteToBinlog = x.isNoWriteToBinlog();
     boolean local = x.isLocal();

     final List<SQLExprTableSource> tableSources = x.getTableSources();

    return false;
  }

  @Override
  public void endVisit(MySqlOptimizeStatement x) {

     boolean noWriteToBinlog = x.isNoWriteToBinlog();
     boolean local = x.isLocal();

     final List<SQLExprTableSource> tableSources = x.getTableSources();
  }

  @Override
  public boolean visit(MySqlHintStatement x) {
     List<SQLCommentHint> hints = x.getHints();
    return false;
  }

  @Override
  public void endVisit(MySqlHintStatement x) {
     List<SQLCommentHint> hints = x.getHints();
  }

  @Override
  public boolean visit(MySqlOrderingExpr x) {
     SQLExpr expr = x.getExpr();
     SQLOrderingSpecification type = x.getType();
    return false;
  }

  @Override
  public void endVisit(MySqlOrderingExpr x) {
     SQLExpr expr = x.getExpr();
     SQLOrderingSpecification type = x.getType();
  }

  @Override
  public boolean visit(MySqlCaseStatement x) {
     SQLExpr condition = x.getCondition();
    //when statement list
     List<MySqlWhenStatement> whenList = x.getWhenList();
    //else statement
     SQLIfStatement.Else elseItem = x.getElseItem();
    return false;
  }

  @Override
  public void endVisit(MySqlCaseStatement x) {
     SQLExpr condition = x.getCondition();
    //when statement list
     List<MySqlWhenStatement> whenList = x.getWhenList();
    //else statement
     SQLIfStatement.Else elseItem = x.getElseItem();
  }

  @Override
  public boolean visit(MySqlDeclareStatement x) {
     List<SQLDeclareItem> varList = x.getVarList();
    return false;
  }

  @Override
  public void endVisit(MySqlDeclareStatement x) {
     List<SQLDeclareItem> varList = x.getVarList();

  }

  @Override
  public boolean visit(MySqlSelectIntoStatement x) {
    //select statement
     SQLSelect select = x.getSelect();
    //var list
     List<SQLExpr> varList = x.getVarList();
    return false;
  }

  @Override
  public void endVisit(MySqlSelectIntoStatement x) {
     SQLSelect select = x.getSelect();
    //var list
     List<SQLExpr> varList = x.getVarList();
  }

  @Override
  public boolean visit(MySqlWhenStatement x) {
     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
    return false;
  }

  @Override
  public void endVisit(MySqlWhenStatement x) {
     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
  }

  @Override
  public boolean visit(MySqlLeaveStatement x) {
     String labelName = x.getLabelName();
    return false;
  }

  @Override
  public void endVisit(MySqlLeaveStatement x) {
     String labelName = x.getLabelName();

  }

  @Override
  public boolean visit(MySqlIterateStatement x) {
     String labelName = x.getLabelName();
    return false;
  }

  @Override
  public void endVisit(MySqlIterateStatement x) {
     String labelName = x.getLabelName();


  }

  @Override
  public boolean visit(MySqlRepeatStatement x) {

     String labelName = x.getLabelName();

     List<SQLStatement> statements = x.getStatements();

     SQLExpr condition = x.getCondition();
    return false;
  }

  @Override
  public void endVisit(MySqlRepeatStatement x) {
     String labelName = x.getLabelName();

     List<SQLStatement> statements = x.getStatements();

     SQLExpr condition = x.getCondition();
  }

  @Override
  public boolean visit(MySqlCursorDeclareStatement x) {

    //cursor name
     SQLName cursorName = x.getCursorName();
    //select statement
     SQLSelect select = x.getSelect();
    return false;
  }

  @Override
  public void endVisit(MySqlCursorDeclareStatement x) {
    //cursor name
     SQLName cursorName = x.getCursorName();
    //select statement
     SQLSelect select = x.getSelect();
  }

  @Override
  public boolean visit(MySqlUpdateTableSource x) {

     MySqlUpdateStatement update = x.getUpdate();

    return false;
  }

  @Override
  public void endVisit(MySqlUpdateTableSource x) {
     MySqlUpdateStatement update = x.getUpdate();
  }

  @Override
  public boolean visit(MySqlAlterTableAlterColumn x) {

     SQLName column = x.getColumn();

     boolean dropDefault = x.isDropDefault();
     SQLExpr defaultExpr = x.getDefaultExpr();

    return false;
  }

  @Override
  public void endVisit(MySqlAlterTableAlterColumn x) {

     SQLName column = x.getColumn();

     boolean dropDefault = x.isDropDefault();
     SQLExpr defaultExpr = x.getDefaultExpr();
  }

  @Override
  public boolean visit(MySqlSubPartitionByKey x) {
     List<SQLName> columns = x.getColumns();
     short algorithm = x.getAlgorithm();
    return false;
  }

  @Override
  public void endVisit(MySqlSubPartitionByKey x) {
     List<SQLName> columns = x.getColumns();
     short algorithm = x.getAlgorithm();
  }

  @Override
  public boolean visit(MySqlSubPartitionByList x) {

     SQLExpr expr = x.getExpr();

     List<SQLColumnDefinition> columns = x.getColumns();

    return false;
  }

  @Override
  public void endVisit(MySqlSubPartitionByList x) {

     SQLExpr expr = x.getExpr();

     List<SQLColumnDefinition> columns = x.getColumns();
  }

  @Override
  public boolean visit(MySqlDeclareHandlerStatement x) {

    //command type
     MySqlHandlerType handleType = x.getHandleType();
    //sp statement
     SQLStatement spStatement = x.getSpStatement();

     List<ConditionValue> conditionValues = x.getConditionValues();
    return false;
  }

  @Override
  public void endVisit(MySqlDeclareHandlerStatement x) {
    //command type
     MySqlHandlerType handleType = x.getHandleType();
    //sp statement
     SQLStatement spStatement = x.getSpStatement();

     List<ConditionValue> conditionValues = x.getConditionValues();
  }

  @Override
  public boolean visit(MySqlDeclareConditionStatement x) {
    //condition_name
     String conditionName = x.getConditionName();
    //sp statement
     ConditionValue conditionValue = x.getConditionValue();
    return false;
  }

  @Override
  public void endVisit(MySqlDeclareConditionStatement x) {
    //condition_name
     String conditionName = x.getConditionName();
    //sp statement
     ConditionValue conditionValue = x.getConditionValue();
  }

  @Override
  public boolean visit(MySqlFlushStatement x) {
     boolean noWriteToBinlog = x.isNoWriteToBinlog();
     boolean local = x.isLocal();

     final List<SQLExprTableSource> tables = x.getTables();

     boolean withReadLock = x.isWithReadLock();
     boolean forExport = x.isForExport();

     boolean binaryLogs = x.isBinaryLogs();
     boolean desKeyFile = x.isDesKeyFile();
     boolean engineLogs= x.isEngineLogs();
     boolean errorLogs = x.isErrorLogs();
     boolean generalLogs = x.isGeneralLogs();
     boolean hots = x.isHots();
     boolean logs = x.isLogs();
     boolean privileges = x.isPrivileges();
     boolean optimizerCosts = x.isOptimizerCosts();
     boolean queryCache = x.isQueryCache();
     boolean relayLogs = x.isRelayLogs();
     SQLExpr relayLogsForChannel = x.getRelayLogsForChannel();
     boolean slowLogs = x.isSlowLogs();
     boolean status = x.isStatus();
     boolean userResources = x.isUserResources();
     boolean tableOption = x.isTableOption();
    return false;
  }

  @Override
  public void endVisit(MySqlFlushStatement x) {
     boolean noWriteToBinlog = x.isNoWriteToBinlog();
     boolean local = x.isLocal();

     final List<SQLExprTableSource> tables = x.getTables();

     boolean withReadLock = x.isWithReadLock();
     boolean forExport = x.isForExport();

     boolean binaryLogs = x.isBinaryLogs();
     boolean desKeyFile = x.isDesKeyFile();
     boolean engineLogs =x.isEngineLogs();
     boolean errorLogs = x.isErrorLogs();
     boolean generalLogs = x.isGeneralLogs();
     boolean hots = x.isHots();
     boolean logs = x.isLogs();
     boolean privileges = x.isPrivileges();
     boolean optimizerCosts = x.isOptimizerCosts();
     boolean queryCache = x.isQueryCache();
     boolean relayLogs = x.isRelayLogs();
     SQLExpr relayLogsForChannel = x.getRelayLogsForChannel();
     boolean slowLogs = x.isSlowLogs();
     boolean status = x.isStatus();
     boolean userResources = x.isUserResources();
     boolean tableOption = x.isTableOption();
  }

  @Override
  public boolean visit(MySqlEventSchedule x) {
     SQLExpr at = x.getAt();
     SQLExpr every = x.getEvery();
     SQLExpr starts = x.getStarts();
     SQLExpr ends = x.getEnds();
    return false;
  }

  @Override
  public void endVisit(MySqlEventSchedule x) {
     SQLExpr at = x.getAt();
     SQLExpr every = x.getEvery();
     SQLExpr starts = x.getStarts();
     SQLExpr ends = x.getEnds();
  }

  @Override
  public boolean visit(MySqlCreateEventStatement x) {
     SQLName definer = x.getDefiner();
     SQLName name = x.getName();

     boolean ifNotExists = x.isIfNotExists();

     MySqlEventSchedule schedule = x.getSchedule();
     boolean onCompletionPreserve = x.isOnCompletionPreserve();
     SQLName renameTo = x.getRenameTo();
     Boolean enable = x.getEnable();
     boolean disableOnSlave = x.isDisableOnSlave();
     SQLExpr comment = x.getComment();
     SQLStatement eventBody = x.getEventBody();
    return false;
  }

  @Override
  public void endVisit(MySqlCreateEventStatement x) {
     SQLName definer = x.getDefiner();
     SQLName name = x.getName();

     boolean ifNotExists = x.isIfNotExists();

     MySqlEventSchedule schedule = x.getSchedule();
     boolean onCompletionPreserve = x.isOnCompletionPreserve();
     SQLName renameTo = x.getRenameTo();
     Boolean enable = x.getEnable();
     boolean disableOnSlave = x.isDisableOnSlave();
     SQLExpr comment = x.getComment();
     SQLStatement eventBody = x.getEventBody();
  }

  @Override
  public boolean visit(MySqlCreateAddLogFileGroupStatement x) {
     SQLName name = x.getName();
     SQLExpr addUndoFile = x.getAddUndoFile();
     SQLExpr initialSize = x.getInitialSize();
     SQLExpr undoBufferSize = x.getUndoBufferSize();
     SQLExpr redoBufferSize = x.getRedoBufferSize();
     SQLExpr nodeGroup = x.getNodeGroup();
     boolean wait = x.isWait();
     SQLExpr comment = x.getComment();
     SQLExpr engine = x.getEngine();
    return false;
  }

  @Override
  public void endVisit(MySqlCreateAddLogFileGroupStatement x) {
     SQLName name = x.getName();
     SQLExpr addUndoFile = x.getAddUndoFile();
     SQLExpr initialSize = x.getInitialSize();
     SQLExpr undoBufferSize = x.getUndoBufferSize();
     SQLExpr redoBufferSize = x.getRedoBufferSize();
     SQLExpr nodeGroup = x.getNodeGroup();
     boolean wait = x.isWait();
     SQLExpr comment = x.getComment();
     SQLExpr engine = x.getEngine();
  }

  @Override
  public boolean visit(MySqlCreateServerStatement x) {
     SQLName name = x.getName();
     SQLName foreignDataWrapper = x.getForeignDataWrapper();
     SQLExpr host = x.getHost();
     SQLExpr database = x.getDatabase();
     SQLExpr user = x.getUser();
     SQLExpr password = x.getPassword();
     SQLExpr socket = x.getSocket();
     SQLExpr owner = x.getOwner();
     SQLExpr port = x.getPort();
    return false;
  }

  @Override
  public void endVisit(MySqlCreateServerStatement x) {
     SQLName name = x.getName();
     SQLName foreignDataWrapper = x.getForeignDataWrapper();
     SQLExpr host = x.getHost();
     SQLExpr database = x.getDatabase();
     SQLExpr user = x.getUser();
     SQLExpr password = x.getPassword();
     SQLExpr socket = x.getSocket();
     SQLExpr owner = x.getOwner();
     SQLExpr port = x.getPort();
  }

  @Override
  public boolean visit(MySqlCreateTableSpaceStatement x) {
     SQLName name = x.getName();
     SQLExpr addDataFile = x.getAddDataFile();
     SQLExpr initialSize = x.getInitialSize();
     SQLExpr extentSize = x.getExtentSize();
     SQLExpr autoExtentSize = x.getAutoExtentSize();
     SQLExpr fileBlockSize = x.getFileBlockSize();
     SQLExpr logFileGroup = x.getLogFileGroup();
     SQLExpr maxSize = x.getMaxSize();
     SQLExpr nodeGroup = x.getNodeGroup();
     boolean wait = x.isWait();
     SQLExpr comment = x.getComment();
     SQLExpr engine = x.getEngine();
    return false;
  }

  @Override
  public void endVisit(MySqlCreateTableSpaceStatement x) {
     SQLName name = x.getName();
     SQLExpr addDataFile = x.getAddDataFile();
     SQLExpr initialSize = x.getInitialSize();
     SQLExpr extentSize = x.getExtentSize();
     SQLExpr autoExtentSize = x.getAutoExtentSize();
     SQLExpr fileBlockSize = x.getFileBlockSize();
     SQLExpr logFileGroup = x.getLogFileGroup();
     SQLExpr maxSize = x.getMaxSize();
     SQLExpr nodeGroup = x.getNodeGroup();
     boolean wait = x.isWait();
     SQLExpr comment = x.getComment();
     SQLExpr engine = x.getEngine();
  }

  @Override
  public boolean visit(MySqlAlterEventStatement x) {
     SQLName definer = x.getDefiner();
     SQLName name = x.getName();

     MySqlEventSchedule schedule = x.getSchedule();
     boolean onCompletionPreserve = x.isOnCompletionPreserve();
     SQLName renameTo = x.getRenameTo();
     Boolean enable = x.getEnable();
     boolean disableOnSlave = x.isDisableOnSlave();
     SQLExpr comment = x.getComment();
     SQLStatement eventBody = x.getEventBody();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterEventStatement x) {
     SQLName definer = x.getDefiner();
     SQLName name = x.getName();

     MySqlEventSchedule schedule = x.getSchedule();
     boolean onCompletionPreserve = x.isOnCompletionPreserve();
     SQLName renameTo = x.getRenameTo();
     Boolean enable = x.getEnable();
     boolean disableOnSlave = x.isDisableOnSlave();
     SQLExpr comment = x.getComment();
     SQLStatement eventBody = x.getEventBody();
  }

  @Override
  public boolean visit(MySqlAlterLogFileGroupStatement x) {
     SQLName name = x.getName();
     SQLExpr addUndoFile = x.getAddUndoFile();
     SQLExpr initialSize = x.getInitialSize();
     boolean wait = x.isWait();
     SQLExpr engine = x.getEngine();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterLogFileGroupStatement x) {
     SQLName name = x.getName();
     SQLExpr addUndoFile = x.getAddUndoFile();
     SQLExpr initialSize = x.getInitialSize();
     boolean wait = x.isWait();
     SQLExpr engine = x.getEngine();
  }

  @Override
  public boolean visit(MySqlAlterServerStatement x) {
     SQLName name = x.getName();

    // options
     SQLExpr user = x.getUser();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterServerStatement x) {
     SQLName name = x.getName();

    // options
     SQLExpr user = x.getUser();
  }

  @Override
  public boolean visit(MySqlAlterTablespaceStatement x) {
     SQLName name = x.getName();

     SQLExpr addDataFile = x.getAddDataFile();
     SQLExpr dropDataFile = x.getDropDataFile();
     SQLExpr initialSize = x.getInitialSize();
     boolean wait = x.isWait();
     SQLExpr engine = x.getEngine();
    return false;
  }

  @Override
  public void endVisit(MySqlAlterTablespaceStatement x) {
     SQLName name = x.getName();

     SQLExpr addDataFile = x.getAddDataFile();
     SQLExpr dropDataFile = x.getDropDataFile();
     SQLExpr initialSize = x.getInitialSize();
     boolean wait = x.isWait();
     SQLExpr engine = x.getEngine();
  }

  @Override
  public boolean visit(MySqlShowDatabasePartitionStatusStatement x) {

     SQLName database = x.getDatabase();
    return false;
  }

  @Override
  public void endVisit(MySqlShowDatabasePartitionStatusStatement x) {
     SQLName database = x.getDatabase();

  }

  @Override
  public boolean visit(MySqlChecksumTableStatement x) {

     boolean quick = x.isQuick();
     boolean extended = x.isExtended();
    return false;
  }

  @Override
  public void endVisit(MySqlChecksumTableStatement x) {

     boolean quick = x.isQuick();
     boolean extended = x.isExtended();
  }

  @Override
  public void endVisit(SQLAllColumnExpr x) {
    SQLTableSource resolvedTableSource = x.getResolvedTableSource();

  }

  @Override
  public void endVisit(SQLBetweenExpr x) {
     SQLExpr            testExpr  = x.getTestExpr();
     boolean           not =x.isNot();
     SQLExpr            beginExpr =x.getBeginExpr();
     SQLExpr            endExpr =x.getEndExpr();
  }

  @Override
  public void endVisit(SQLBinaryOpExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr left = x.getLeft();
    SQLExpr right = x.getRight();
    SQLBinaryOperator operator = x.getOperator();
  }

  @Override
  public void endVisit(SQLCaseExpr x) {
    SQLExpr elseExpr = x.getElseExpr();
    SQLExpr valueExpr = x.getValueExpr();
    List<SQLCaseExpr.Item> items = x.getItems();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLCaseExpr.Item x) {
    SQLExpr conditionExpr = x.getConditionExpr();
    SQLExpr statement = x.getValueExpr();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLCaseStatement x) {
    SQLExpr valueExpr = x.getValueExpr();
    List<SQLStatement> elseStatements = x.getElseStatements();
    List<SQLCaseStatement.Item> items = x.getItems();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLCaseStatement.Item x) {
    SQLExpr conditionExpr = x.getConditionExpr();
    SQLStatement statement = x.getStatement();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLCharExpr x) {
    List<SQLObject> children = x.getChildren();
    Object value = x.getValue();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLIdentifierExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLTableSource tableSource = x.getResolvedTableSource();
    SQLColumnDefinition column = x.getResolvedColumn();
    SQLDeclareItem declareItem = x.getResolvedDeclareItem();
    SQLObject ownerObject = x.getResolvedOwnerObject();
    SQLParameter parameter = x.getResolvedParameter();
  }

  @Override
  public void endVisit(SQLInListExpr x) {
    SQLExpr expr = x.getExpr();
    SQLDataType sqlDataType = x.computeDataType();
    List<SQLExpr> targetList = x.getTargetList();
  }

  @Override
  public void endVisit(SQLIntegerExpr x) {
    Number number = x.getNumber();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLExistsExpr x) {
    boolean not = x.isNot();
    SQLSelect subQuery = x.getSubQuery();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLNCharExpr x) {
    String text = x.getText();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLNotExpr x) {
    SQLExpr expr = x.getExpr();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLNullExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    Object value = x.getValue();
  }

  @Override
  public void endVisit(SQLNumberExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    Number number = x.getNumber();
  }

  @Override
  public void endVisit(SQLPropertyExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr owner = x.getOwner();
    SQLColumnDefinition resolvedColumn = x.getResolvedColumn();
    SQLCreateProcedureStatement resolvedProcudure = x.getResolvedProcudure();
    SQLTableSource resolvedTableSource = x.getResolvedTableSource();

  }

  @Override
  public void endVisit(SQLSelectGroupByClause x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr having = x.getHaving();
    List<SQLExpr> items = x.getItems();
  }

  @Override
  public void endVisit(SQLSelectItem x) {
    String alias = x.getAlias();
    SQLExpr expr = x.getExpr();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public void endVisit(SQLSelectStatement selectStatement) {
    SQLSelect select = selectStatement.getSelect();
    SQLDataType sqlDataType = selectStatement.computeDataType();


  }

  @Override
  public void postVisit(SQLObject x) {

  }

  @Override
  public void preVisit(SQLObject x) {

  }

  @Override
  public boolean visit(SQLAllColumnExpr x) {
    SQLTableSource resolvedTableSource = x.getResolvedTableSource();
    SQLDataType sqlDataType = x.computeDataType();
    return false;
  }

  @Override
  public boolean visit(SQLBetweenExpr x) {
    SQLExpr testExpr = x.getTestExpr();
    boolean not = x.isNot();
    SQLExpr beginExpr = x.getBeginExpr();
    SQLExpr endExpr = x.getEndExpr();
    SQLDataType sqlDataType = x.computeDataType();
    return false;
  }

  @Override
  public boolean visit(SQLBinaryOpExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr left = x.getLeft();
    SQLExpr right = x.getRight();
    SQLBinaryOperator operator = x.getOperator();

    return false;
  }

  @Override
  public boolean visit(SQLCaseExpr x) {
    SQLExpr elseExpr = x.getElseExpr();
    SQLExpr valueExpr = x.getValueExpr();
    List<SQLCaseExpr.Item> items = x.getItems();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLCaseExpr.Item x) {
    SQLExpr conditionExpr = x.getConditionExpr();
    SQLExpr valueExpr = x.getValueExpr();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLCaseStatement x) {
    SQLExpr valueExpr = x.getValueExpr();
    List<SQLStatement> elseStatements = x.getElseStatements();
    List<SQLCaseStatement.Item> items = x.getItems();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLCaseStatement.Item x) {
    SQLExpr conditionExpr = x.getConditionExpr();
    SQLStatement statement = x.getStatement();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLCastExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr expr = x.getExpr();

    return false;
  }

  @Override
  public boolean visit(SQLCharExpr x) {
    Object value = x.getValue();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLExistsExpr x) {
    boolean not = x.isNot();
    SQLSelect subQuery = x.getSubQuery();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLTableSource tableSource = x.getResolvedTableSource();
    SQLColumnDefinition column = x.getResolvedColumn();
    SQLDeclareItem declareItem = x.getResolvedDeclareItem();
    SQLObject ownerObject = x.getResolvedOwnerObject();
    SQLParameter parameter = x.getResolvedParameter();

    return false;
  }

  @Override
  public boolean visit(SQLInListExpr x) {
    SQLExpr expr = x.getExpr();
    SQLDataType sqlDataType = x.computeDataType();
    List<SQLExpr> targetList = x.getTargetList();

    return false;
  }

  @Override
  public boolean visit(SQLIntegerExpr x) {
    Number number = x.getNumber();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLNCharExpr x) {
    String text = x.getText();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLNotExpr x) {
    SQLExpr expr = x.getExpr();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public boolean visit(SQLNullExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    Object value = x.getValue();

    return false;
  }

  @Override
  public boolean visit(SQLNumberExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    Number number = x.getNumber();

    return false;
  }

  @Override
  public boolean visit(SQLPropertyExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr owner = x.getOwner();
    SQLColumnDefinition resolvedColumn = x.getResolvedColumn();
    SQLCreateProcedureStatement resolvedProcudure = x.getResolvedProcudure();
    SQLTableSource resolvedTableSource = x.getResolvedTableSource();

    return false;
  }

  @Override
  public boolean visit(SQLSelectGroupByClause x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr having = x.getHaving();
    List<SQLExpr> items = x.getItems();

    return false;
  }

  @Override
  public boolean visit(SQLSelectItem x) {
    String alias = x.getAlias();
    SQLExpr expr = x.getExpr();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public void endVisit(SQLCastExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(SQLSelectStatement selectStatement) {
    SQLSelect select = selectStatement.getSelect();
    SQLDataType sqlDataType = selectStatement.computeDataType();

    return false;
  }

  @Override
  public void endVisit(SQLAggregateExpr astNode) {
    String methodName = astNode.getMethodName();
    SQLAggregateOption option = astNode.getOption();
    final List<SQLExpr> arguments = astNode.getArguments();
    SQLKeep keep = astNode.getKeep();
    SQLExpr filter = astNode.getFilter();
    SQLOver over = astNode.getOver();
    SQLName overRef = astNode.getOverRef();
    SQLOrderBy withinGroup = astNode.getWithinGroup();
    Boolean ignoreNulls = astNode.getIgnoreNulls();
  }

  @Override
  public boolean visit(SQLAggregateExpr astNode) {
    SQLDataType sqlDataType = astNode.computeDataType();
    String methodName = astNode.getMethodName();
    SQLAggregateOption option = astNode.getOption();
    final List<SQLExpr> arguments = astNode.getArguments();
    SQLKeep keep = astNode.getKeep();
    SQLExpr filter = astNode.getFilter();
    SQLOver over = astNode.getOver();
    SQLName overRef = astNode.getOverRef();
    SQLOrderBy withinGroup = astNode.getWithinGroup();
    Boolean ignoreNulls = astNode.getIgnoreNulls();

    return false;
  }

  @Override
  public boolean visit(SQLVariantRefExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    int index = x.getIndex();
    String name = x.getName();

    return false;
  }

  @Override
  public void endVisit(SQLVariantRefExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    int index = x.getIndex();
    String name = x.getName();
  }

  @Override
  public boolean visit(SQLQueryExpr x) {
    SQLSelect subQuery = x.getSubQuery();
    SQLDataType sqlDataType = x.computeDataType();

    return false;
  }

  @Override
  public void endVisit(SQLQueryExpr x) {
    SQLSelect subQuery = x.getSubQuery();
    SQLDataType sqlDataType = x.computeDataType();
  }

  @Override
  public boolean visit(SQLUnaryExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr expr = x.getExpr();
    SQLUnaryOperator operator = x.getOperator();

    return false;
  }

  @Override
  public void endVisit(SQLUnaryExpr x) {
    SQLDataType sqlDataType = x.computeDataType();
    SQLExpr expr = x.getExpr();
    SQLUnaryOperator operator = x.getOperator();
  }

  @Override
  public boolean visit(SQLHexExpr x) {
    byte[] value = x.getValue();
    return false;
  }

  @Override
  public void endVisit(SQLHexExpr x) {
    byte[] value = x.getValue();
  }

  @Override
  public boolean visit(SQLSelect x) {
    boolean simple = x.isSimple();
    SQLOrderBy orderBy = x.getOrderBy();
    SQLExpr offset = x.getOffset();
    boolean forBrowse = x.isForBrowse();
    SQLWithSubqueryClause withSubQuery = x.getWithSubQuery();

    return false;
  }

  @Override
  public void endVisit(SQLSelect x) {
    boolean simple = x.isSimple();
    SQLOrderBy orderBy = x.getOrderBy();
    SQLExpr offset = x.getOffset();
    boolean forBrowse = x.isForBrowse();
    SQLWithSubqueryClause withSubQuery = x.getWithSubQuery();

  }

  @Override
  public boolean visit(SQLSelectQueryBlock x) {
    boolean bracket = x.isBracket();
    int distionOption = x.getDistionOption();
    final List<SQLSelectItem> selectList = x.getSelectList();

    SQLTableSource from = x.getFrom();
    SQLExprTableSource into = x.getInto();
    SQLExpr where = x.getWhere();

    SQLSelectGroupByClause groupBy = x.getGroupBy();
    List<SQLWindow> windows = x.getWindows();
    SQLOrderBy orderBy = x.getOrderBy();
    boolean parenthesized = x.isParenthesized();
    boolean forUpdate = x.isForUpdate();
    boolean noWait = x.isNoWait();
    SQLExpr waitTime = x.getWaitTime();
    SQLLimit limit = x.getLimit();

    return false;
  }

  @Override
  public void endVisit(SQLSelectQueryBlock x) {
    boolean bracket = x.isBracket();
    int distionOption = x.getDistionOption();
    final List<SQLSelectItem> selectList = x.getSelectList();

    SQLTableSource from = x.getFrom();
    SQLExprTableSource into = x.getInto();
    SQLExpr where = x.getWhere();

    SQLSelectGroupByClause groupBy = x.getGroupBy();
    List<SQLWindow> windows = x.getWindows();
    SQLOrderBy orderBy = x.getOrderBy();
    boolean parenthesized = x.isParenthesized();
    boolean forUpdate = x.isForUpdate();
    boolean noWait = x.isNoWait();
    SQLExpr waitTime = x.getWaitTime();
    SQLLimit limit = x.getLimit();
  }

  @Override
  public boolean visit(SQLExprTableSource x) {
    SQLExpr expr = x.getExpr();
    List<SQLName> partitions = x.getPartitions();
    SchemaObject schemaObject = x.getSchemaObject();

    return false;
  }

  @Override
  public void endVisit(SQLExprTableSource x) {
    SQLExpr expr = x.getExpr();
    List<SQLName> partitions = x.getPartitions();
    SchemaObject schemaObject = x.getSchemaObject();
  }

  @Override
  public boolean visit(SQLOrderBy x) {
    List<SQLSelectOrderByItem> items = x.getItems();

    return false;
  }

  @Override
  public void endVisit(SQLOrderBy x) {
    List<SQLSelectOrderByItem> items = x.getItems();
  }

  @Override
  public boolean visit(SQLSelectOrderByItem x) {
    SQLExpr expr = x.getExpr();
    String collate = x.getCollate();
    SQLOrderingSpecification type = x.getType();
    NullsOrderType nullsOrderType = x.getNullsOrderType();

    SQLSelectItem resolvedSelectItem = x.getResolvedSelectItem();

    return false;
  }

  @Override
  public void endVisit(SQLSelectOrderByItem x) {
    SQLExpr expr = x.getExpr();
    String collate = x.getCollate();
    SQLOrderingSpecification type = x.getType();
    NullsOrderType nullsOrderType = x.getNullsOrderType();

    SQLSelectItem resolvedSelectItem = x.getResolvedSelectItem();

  }

  @Override
  public boolean visit(SQLDropTableStatement x) {
     List<SQLCommentHint> hints = x.getHints();

     List<SQLExprTableSource> tableSources = x.getTableSources();

     boolean purge = x.isPurge();

     boolean cascade = x.isCascade();
     boolean restrict = x.isRestrict();
     boolean ifExists = x.isIfExists();
     boolean temporary = x.isTemporary();
    return false;
  }

  @Override
  public void endVisit(SQLDropTableStatement x) {
     List<SQLCommentHint> hints = x.getHints();

     List<SQLExprTableSource> tableSources = x.getTableSources();

     boolean purge = x.isPurge();

     boolean cascade = x.isCascade();
     boolean restrict = x.isRestrict();
     boolean ifExists = x.isIfExists();
     boolean temporary = x.isTemporary();
  }

  @Override
  public boolean visit(SQLCreateTableStatement x) {
     boolean ifNotExiists = x.isIfNotExiists();
     SQLCreateTableStatement.Type type;
     SQLExprTableSource tableSource = x.getTableSource();

     List<SQLTableElement> tableElementList = x.getTableElementList();

    return false;
  }

  @Override
  public void endVisit(SQLCreateTableStatement x) {
     boolean ifNotExiists = x.isIfNotExiists();
     SQLCreateTableStatement.Type type;
     SQLExprTableSource tableSource = x.getTableSource();

     List<SQLTableElement> tableElementList = x.getTableElementList();


  }

  @Override
  public boolean visit(SQLColumnDefinition x) {
     String dbType = x.getDbType();

     SQLName name = x.getName();
     SQLDataType dataType = x.getDataType();
     SQLExpr defaultExpr = x.getDefaultExpr();
     final List<SQLColumnConstraint> constraints = x.getConstraints();
     SQLExpr comment = x.getComment();

     Boolean enable = x.getEnable();
     Boolean validate = x.getValidate();
     Boolean rely = x.getRely();

    // for mysql
     boolean autoIncrement = x.isAutoIncrement();
     SQLExpr onUpdate = x.getOnUpdate();
     SQLExpr storage = x.getStorage();
     SQLExpr charsetExpr = x.getCharsetExpr();
     SQLExpr asExpr = x.getAsExpr();
     boolean stored = x.isStored();
     boolean virtual = x.isVirtual();

     Identity identity = x.getIdentity();
     SQLExpr generatedAlawsAs = x.getGeneratedAlawsAs();

    return false;
  }

  @Override
  public void endVisit(SQLColumnDefinition x) {
     String dbType = x.getDbType();

     SQLName name = x.getName();
     SQLDataType dataType = x.getDataType();
     SQLExpr defaultExpr = x.getDefaultExpr();
     final List<SQLColumnConstraint> constraints = x.getConstraints();
     SQLExpr comment = x.getComment();

     Boolean enable = x.getEnable();
     Boolean validate = x.getValidate();
     Boolean rely = x.getRely();

    // for mysql
     boolean autoIncrement = x.isAutoIncrement();
     SQLExpr onUpdate = x.getOnUpdate();
     SQLExpr storage = x.getStorage();
     SQLExpr charsetExpr = x.getCharsetExpr();
     SQLExpr asExpr = x.getAsExpr();
     boolean stored = x.isStored();
     boolean virtual = x.isVirtual();

     Identity identity = x.getIdentity();
     SQLExpr generatedAlawsAs = x.getGeneratedAlawsAs();
  }

  @Override
  public boolean visit(Identity x) {
     Integer seed = x.getSeed();
     Integer increment = x.getIncrement();

     boolean notForReplication = x.isNotForReplication();
    return false;
  }

  @Override
  public void endVisit(Identity x) {
     Integer seed = x.getSeed();
     Integer increment = x.getIncrement();

     boolean notForReplication = x.isNotForReplication();
  }

  @Override
  public boolean visit(SQLDataType x) {
    List<SQLExpr> arguments = x.getArguments();
    String dbType = x.getDbType();
    String name = x.getName();
    Boolean withTimeZone = x.getWithTimeZone();
    boolean withLocalTimeZone = x.isWithLocalTimeZone();
    return false;
  }

  @Override
  public void endVisit(SQLDataType x) {
    List<SQLExpr> arguments = x.getArguments();
    String dbType = x.getDbType();
    String name = x.getName();
    Boolean withTimeZone = x.getWithTimeZone();
    boolean withLocalTimeZone = x.isWithLocalTimeZone();
  }

  @Override
  public boolean visit(SQLCharacterDataType x) {
     String charSetName = x.getCharSetName();
     String collate = x.getCollate();

     String charType = x.getCharType();
     boolean hasBinary = x.isHasBinary();

     List<SQLCommentHint> hints = x.getHints();
    return false;
  }

  @Override
  public void endVisit(SQLCharacterDataType x) {
     String charSetName = x.getCharSetName();
     String collate = x.getCollate();

     String charType = x.getCharType();
     boolean hasBinary = x.isHasBinary();

     List<SQLCommentHint> hints = x.getHints();
  }

  @Override
  public boolean visit(SQLDeleteStatement x) {
     SQLWithSubqueryClause with = x.getWith();

     SQLTableSource tableSource = x.getTableSource();
     SQLExpr where = x.getWhere();
     SQLTableSource from = x.getFrom();
     SQLTableSource using = x.getUsing();

     boolean only = x.isOnly();

    return false;
  }

  @Override
  public void endVisit(SQLDeleteStatement x) {
     SQLWithSubqueryClause with = x.getWith();

     SQLTableSource tableSource = x.getTableSource();
     SQLExpr where = x.getWhere();
     SQLTableSource from = x.getFrom();
     SQLTableSource using = x.getUsing();

     boolean only = x.isOnly();

  }

  @Override
  public boolean visit(SQLCurrentOfCursorExpr x) {
     SQLName cursorName = x.getCursorName();
    return false;
  }

  @Override
  public void endVisit(SQLCurrentOfCursorExpr x) {
     SQLName cursorName = x.getCursorName();
  }

  @Override
  public boolean visit(SQLInsertStatement x) {
     SQLWithSubqueryClause with = x.getWith();

     String dbType = x.getDbType();

     boolean upsert = x.isUpsert(); // for phoenix

     boolean afterSemi = x.isAfterSemi();

     List<SQLCommentHint> headHints = x.getHeadHintsDirect();

    return false;
  }

  @Override
  public void endVisit(SQLInsertStatement x) {
     SQLWithSubqueryClause with = x.getWith();

     String dbType = x.getDbType();

     boolean upsert = x.isUpsert(); // for phoenix

     boolean afterSemi = x.isAfterSemi();

     List<SQLCommentHint> headHints = x.getHeadHintsDirect();
  }

  @Override
  public boolean visit(ValuesClause x) {
     final List<SQLExpr> values = x.getValues();
      String originalString = x.getOriginalString();
      int replaceCount = x.getReplaceCount();
    return false;
  }

  @Override
  public void endVisit(ValuesClause x) {
     final List<SQLExpr> values = x.getValues();
      String originalString = x.getOriginalString();
      int replaceCount = x.getReplaceCount();
  }

  @Override
  public boolean visit(SQLUpdateSetItem x) {

     SQLExpr column = x.getColumn();
     SQLExpr value = x.getValue();

    return false;
  }

  @Override
  public void endVisit(SQLUpdateSetItem x) {
     SQLExpr column = x.getColumn();
     SQLExpr value = x.getValue();
  }

  @Override
  public boolean visit(SQLUpdateStatement x) {

     final List<SQLUpdateSetItem> items = x.getItems();
     SQLExpr where = x.getWhere();
     SQLTableSource from = x.getFrom();

     SQLTableSource tableSource = x.getTableSource();
     List<SQLExpr> returning = x.getReturning();

     List<SQLHint> hints = x.getHints();

    // for mysql
     SQLOrderBy orderBy = x.getOrderBy();
    return false;
  }

  @Override
  public void endVisit(SQLUpdateStatement x) {

     final List<SQLUpdateSetItem> items = x.getItems();
     SQLExpr where = x.getWhere();
     SQLTableSource from = x.getFrom();

     SQLTableSource tableSource = x.getTableSource();
     List<SQLExpr> returning = x.getReturning();

     List<SQLHint> hints = x.getHints();

    // for mysql
     SQLOrderBy orderBy = x.getOrderBy();
  }

  @Override
  public boolean visit(SQLCreateViewStatement x) {
     boolean orReplace = x.isOrReplace();
     boolean force = x.isForce();
    //  SQLName   name;
     SQLSelect subQuery =x.getSubQuery();
     boolean ifNotExists = x.isIfNotExists();

     String algorithm =x.getAlgorithm();
     SQLName definer =x.getDefiner();
     String sqlSecurity =x.getSqlSecurity();

     SQLExprTableSource tableSource =x.getTableSource();

     final List<SQLTableElement> columns = x.getColumns();

     boolean withCheckOption  =x.isWithCheckOption();
     boolean withCascaded =x.isWithCascaded();
     boolean withLocal =x.isWithLocal();
     boolean withReadOnly = x.isWithReadOnly();

     SQLLiteralExpr comment = x.getComment();
    return false;
  }

  @Override
  public void endVisit(SQLCreateViewStatement x) {
     boolean orReplace = x.isOrReplace();
     boolean force = x.isForce();
    //  SQLName   name;
     SQLSelect subQuery =x.getSubQuery();
     boolean ifNotExists = x.isIfNotExists();

     String algorithm =x.getAlgorithm();
     SQLName definer =x.getDefiner();
     String sqlSecurity =x.getSqlSecurity();

     SQLExprTableSource tableSource =x.getTableSource();

     final List<SQLTableElement> columns = x.getColumns();

     boolean withCheckOption  =x.isWithCheckOption();
     boolean withCascaded =x.isWithCascaded();
     boolean withLocal =x.isWithLocal();
     boolean withReadOnly = x.isWithReadOnly();

     SQLLiteralExpr comment = x.getComment();
  }

  @Override
  public boolean visit(Column x) {

     SQLExpr     expr =x.getExpr();
     SQLCharExpr comment =x.getComment();
    return false;
  }

  @Override
  public void endVisit(Column x) {
     SQLExpr     expr =x.getExpr();
     SQLCharExpr comment =x.getComment();
  }

  @Override
  public boolean visit(SQLNotNullConstraint x) {
    //null
    return false;
  }

  @Override
  public void endVisit(SQLNotNullConstraint x) {
    //null

  }

  @Override
  public void endVisit(SQLMethodInvokeExpr x) {
     String              name =x.getMethodName();
     SQLExpr             owner =x.getOwner();
     final List<SQLExpr> parameters       = x.getParameters();

     SQLExpr             from =x.getFrom();
     SQLExpr             using =x.getUsing();
     SQLExpr             _for =x.getFor();

     String              trimOption =x.getTrimOption();
  }

  @Override
  public boolean visit(SQLMethodInvokeExpr x) {
     String              name =x.getMethodName();
     SQLExpr             owner =x.getOwner();
     final List<SQLExpr> parameters       = x.getParameters();

     SQLExpr             from =x.getFrom();
     SQLExpr             using =x.getUsing();
     SQLExpr             _for =x.getFor();

     String              trimOption =x.getTrimOption();
    return false;
  }

  @Override
  public void endVisit(SQLUnionQuery x) {
     boolean          bracket  = x.isBracket();

     SQLSelectQuery left = x.getLeft();
     SQLSelectQuery   right =x.getRight();
     SQLUnionOperator operator = x.getOperator();
     SQLOrderBy       orderBy =x.getOrderBy();

     SQLLimit         limit =x.getLimit();
  }

  @Override
  public boolean visit(SQLUnionQuery x) {
     boolean          bracket  = x.isBracket();

     SQLSelectQuery left = x.getLeft();
     SQLSelectQuery   right =x.getRight();
     SQLUnionOperator operator = x.getOperator();
     SQLOrderBy       orderBy =x.getOrderBy();

     SQLLimit         limit =x.getLimit();
    return false;
  }

  @Override
  public void endVisit(SQLSetStatement x) {
     SQLSetStatement.Option option = x.getOption();

     List<SQLAssignItem> items = x.getItems();

     List<SQLCommentHint> hints =x.getHints();
  }

  @Override
  public boolean visit(SQLSetStatement x) {
     SQLSetStatement.Option option = x.getOption();

     List<SQLAssignItem> items = x.getItems();

     List<SQLCommentHint> hints =x.getHints();
    return false;
  }

  @Override
  public void endVisit(SQLAssignItem x) {

     SQLExpr target = x.getTarget();
     SQLExpr value =x.getValue();

  }

  @Override
  public boolean visit(SQLAssignItem x) {
     SQLExpr target = x.getTarget();
     SQLExpr value =x.getValue();
    return false;
  }

  @Override
  public void endVisit(SQLCallStatement x) {

     boolean             brace      = x.isBrace();

     SQLVariantRefExpr   outParameter =x.getOutParameter();

     SQLName             procedureName =x.getProcedureName();

     final List<SQLExpr> parameters = x.getParameters();
  }

  @Override
  public boolean visit(SQLCallStatement x) {

     boolean             brace      = x.isBrace();

     SQLVariantRefExpr   outParameter =x.getOutParameter();

     SQLName             procedureName =x.getProcedureName();

     final List<SQLExpr> parameters = x.getParameters();
    return false;
  }

  @Override
  public void endVisit(SQLJoinTableSource x) {
     SQLTableSource      left = x.getLeft();
     JoinType joinType = x.getJoinType();
     SQLTableSource      right = x.getRight();
     SQLExpr             condition =x.getCondition();
     final List<SQLExpr> using = x.getUsing();

  }

  @Override
  public boolean visit(SQLJoinTableSource x) {
     SQLTableSource      left = x.getLeft();
     JoinType joinType = x.getJoinType();
     SQLTableSource      right = x.getRight();
     SQLExpr             condition =x.getCondition();
     final List<SQLExpr> using = x.getUsing();
    return false;
  }

  @Override
  public void endVisit(SQLSomeExpr x) {
     SQLSelect subQuery = x.getSubQuery();
  }

  @Override
  public boolean visit(SQLSomeExpr x) {
     SQLSelect subQuery = x.getSubQuery();

    return false;
  }

  @Override
  public void endVisit(SQLAnyExpr x) {
     SQLSelect subQuery = x.getSubQuery();

  }

  @Override
  public boolean visit(SQLAnyExpr x) {
     SQLSelect subQuery = x.getSubQuery();

    return false;
  }

  @Override
  public void endVisit(SQLAllExpr x) {
     SQLSelect subQuery = x.getSubQuery();

  }

  @Override
  public boolean visit(SQLAllExpr x) {
     SQLSelect subQuery = x.getSubQuery();

    return false;
  }

  @Override
  public void endVisit(SQLInSubQueryExpr x) {
     boolean           not              = x.isNot();
     SQLExpr           expr = x.getExpr();

     SQLSelect          subQuery = x.getSubQuery();

  }

  @Override
  public boolean visit(SQLInSubQueryExpr x) {
     boolean           not              = x.isNot();
     SQLExpr           expr = x.getExpr();

     SQLSelect          subQuery = x.getSubQuery();
    return false;
  }

  @Override
  public void endVisit(SQLListExpr x) {
    List<SQLExpr> items = x.getItems();

  }

  @Override
  public boolean visit(SQLListExpr x) {
    List<SQLExpr> items = x.getItems();
    return false;
  }

  @Override
  public void endVisit(SQLSubqueryTableSource x) {
     SQLSelect select = x.getSelect();
  }

  @Override
  public boolean visit(SQLSubqueryTableSource x) {
     SQLSelect select = x.getSelect();

    return false;
  }

  @Override
  public void endVisit(SQLTruncateStatement x) {
     List<SQLExprTableSource> tableSources               = x.getTableSources();

     boolean                    purgeSnapshotLog           = x.isPurgeSnapshotLog();

     boolean                    only = x.isOnly();
     Boolean                    restartIdentity = x.getRestartIdentity();
     Boolean                    cascade = x.getCascade();
  }

  @Override
  public boolean visit(SQLTruncateStatement x) {
     List<SQLExprTableSource> tableSources               = x.getTableSources();

     boolean                    purgeSnapshotLog           = x.isPurgeSnapshotLog();

     boolean                    only = x.isOnly();
     Boolean                    restartIdentity = x.getRestartIdentity();
     Boolean                    cascade = x.getCascade();
    return false;
  }

  @Override
  public void endVisit(SQLDefaultExpr x) {
    //null
  }

  @Override
  public boolean visit(SQLDefaultExpr x) {
    //null
    return false;
  }

  @Override
  public void endVisit(SQLCommentStatement x) {
     SQLExprTableSource on = x.getOn();
     SQLCommentStatement.Type type = x.getType();
     SQLExpr            comment = x.getComment();
  }

  @Override
  public boolean visit(SQLCommentStatement x) {
     SQLExprTableSource on = x.getOn();
     SQLCommentStatement.Type type = x.getType();
     SQLExpr            comment = x.getComment();
    return false;
  }

  @Override
  public void endVisit(SQLUseStatement x) {
     SQLName database = x.getDatabase();
  }

  @Override
  public boolean visit(SQLUseStatement x) {
     SQLName database = x.getDatabase();
    return false;
  }

  @Override
  public boolean visit(SQLAlterTableAddColumn x) {

     final List<SQLColumnDefinition> columns = x.getColumns();


    // for mysql
     SQLName firstColumn = x.getFirstColumn();
     SQLName afterColumn = x.getAfterColumn();

     boolean first = x.isFirst();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableAddColumn x) {

     final List<SQLColumnDefinition> columns = x.getColumns();


    // for mysql
     SQLName firstColumn = x.getFirstColumn();
     SQLName afterColumn = x.getAfterColumn();

     boolean first = x.isFirst();
  }

  @Override
  public boolean visit(SQLAlterTableDropColumnItem x) {
     List<SQLName> columns = x.getColumns();

     boolean       cascade = x.isCascade();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropColumnItem x) {
     List<SQLName> columns =x.getColumns();

     boolean       cascade = x.isCascade();

  }

  @Override
  public boolean visit(SQLAlterTableDropIndex x) {

     SQLName indexName = x.getIndexName();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropIndex x) {

     SQLName indexName = x.getIndexName();
  }

  @Override
  public boolean visit(SQLDropIndexStatement x) {
     SQLName            indexName = x.getIndexName();
     SQLExprTableSource tableName =x.getTableName();

     SQLExpr            algorithm =x.getAlgorithm();
     SQLExpr            lockOption =x.getLockOption();
    return false;
  }

  @Override
  public void endVisit(SQLDropIndexStatement x) {
     SQLName            indexName = x.getIndexName();
     SQLExprTableSource tableName =x.getTableName();

     SQLExpr            algorithm =x.getAlgorithm();
     SQLExpr            lockOption =x.getLockOption();
  }

  @Override
  public boolean visit(SQLDropViewStatement x) {
     List<SQLExprTableSource> tableSources = x.getTableSources();

     boolean                  cascade      = x.isCascade();
     boolean                  restrict     = x.isRestrict();
     boolean                  ifExists     = x.isIfExists();

    return false;
  }

  @Override
  public void endVisit(SQLDropViewStatement x) {
     List<SQLExprTableSource> tableSources = x.getTableSources();

     boolean                  cascade      = x.isCascade();
     boolean                  restrict     = x.isRestrict();
     boolean                  ifExists     = x.isIfExists();

  }

  @Override
  public boolean visit(SQLSavePointStatement x) {
     SQLExpr name = x.getName();
    return false;
  }

  @Override
  public void endVisit(SQLSavePointStatement x) {
     SQLExpr name = x.getName();
  }

  @Override
  public boolean visit(SQLRollbackStatement x) {

     SQLName to = x.getTo();

    // for mysql
     Boolean chain = x.getChain();
     Boolean release =x.getRelease();
     SQLExpr force =x.getForce();
    return false;
  }

  @Override
  public void endVisit(SQLRollbackStatement x) {
     SQLName to = x.getTo();

    // for mysql
     Boolean chain = x.getChain();
     Boolean release =x.getRelease();
     SQLExpr force =x.getForce();
  }

  @Override
  public boolean visit(SQLReleaseSavePointStatement x) {
     SQLExpr name = x
        .getName();
    return false;
  }

  @Override
  public void endVisit(SQLReleaseSavePointStatement x) {
     SQLExpr name = x
                               .getName();
  }

  @Override
  public void endVisit(SQLCommentHint x) {
     String text = x
        .getText();
  }

  @Override
  public boolean visit(SQLCommentHint x) {
     String text = x
                              .getText();
    return false;
  }

  @Override
  public void endVisit(SQLCreateDatabaseStatement x) {
     SQLName              name =x.getName();

     String               characterSet =x.getCharacterSet();
     String               collate =x.getCollate();

     List<SQLCommentHint> hints =x.getHints();

     boolean            ifNotExists = x.isIfNotExists();
  }

  @Override
  public boolean visit(SQLCreateDatabaseStatement x) {
     SQLName              name =x.getName();

     String               characterSet =x.getCharacterSet();
     String               collate =x.getCollate();

     List<SQLCommentHint> hints =x.getHints();

     boolean            ifNotExists = x.isIfNotExists();
    return false;
  }

  @Override
  public void endVisit(SQLOver x) {
     final List<SQLExpr> partitionBy = x.getPartitionBy();
     SQLOrderBy          orderBy =x.getOrderBy();

    // for db2
     SQLName             of =x.getOf();

     SQLExpr             windowing =x.getWindowing();
     WindowingType windowingType = x.getWindowingType();

     boolean             windowingPreceding =x.isWindowingPreceding();
     boolean             windowingFollowing =x.isWindowingFollowing();

     SQLExpr             windowingBetweenBegin =x.getWindowingBetweenBegin();
     boolean             windowingBetweenBeginPreceding =x.isWindowingBetweenBeginPreceding();
     boolean             windowingBetweenBeginFollowing =x.isWindowingBetweenBeginFollowing();

     SQLExpr             windowingBetweenEnd =x.getWindowingBetweenEnd();
     boolean             windowingBetweenEndPreceding =x.isWindowingBetweenEndPreceding();
     boolean             windowingBetweenEndFollowing =x.isWindowingBetweenEndFollowing();

  }

  @Override
  public boolean visit(SQLOver x) {
     final List<SQLExpr> partitionBy = x.getPartitionBy();
     SQLOrderBy          orderBy =x.getOrderBy();

    // for db2
     SQLName             of =x.getOf();

     SQLExpr             windowing =x.getWindowing();
     WindowingType windowingType = x.getWindowingType();

     boolean             windowingPreceding =x.isWindowingPreceding();
     boolean             windowingFollowing =x.isWindowingFollowing();

     SQLExpr             windowingBetweenBegin =x.getWindowingBetweenBegin();
     boolean             windowingBetweenBeginPreceding =x.isWindowingBetweenBeginPreceding();
     boolean             windowingBetweenBeginFollowing =x.isWindowingBetweenBeginFollowing();

     SQLExpr             windowingBetweenEnd =x.getWindowingBetweenEnd();
     boolean             windowingBetweenEndPreceding =x.isWindowingBetweenEndPreceding();
     boolean             windowingBetweenEndFollowing =x.isWindowingBetweenEndFollowing();
    return false;
  }

  @Override
  public void endVisit(SQLKeep x) {
     DenseRank denseRank = x.getDenseRank();

     SQLOrderBy orderBy =x.getOrderBy();
  }

  @Override
  public boolean visit(SQLKeep x) {
     DenseRank denseRank = x.getDenseRank();

     SQLOrderBy orderBy =x.getOrderBy();
    return false;
  }

  @Override
  public void endVisit(SQLColumnPrimaryKey x) {
//null
  }

  @Override
  public boolean visit(SQLColumnPrimaryKey x) {
    //null
    return false;
  }

  @Override
  public boolean visit(SQLColumnUniqueKey x) {
    //null
    return false;
  }

  @Override
  public void endVisit(SQLColumnUniqueKey x) {
//null
  }

  @Override
  public void endVisit(SQLWithSubqueryClause x) {
     Boolean           recursive  =x.getRecursive();
     final List<Entry> entries = x.getEntries();
  }

  @Override
  public boolean visit(SQLWithSubqueryClause x) {
     Boolean           recursive  =x.getRecursive();
     final List<Entry> entries = x.getEntries();
    return false;
  }

  @Override
  public void endVisit(Entry x) {
     final List<SQLName> columns = x.getColumns();
     SQLSelect           subQuery =x.getSubQuery();
     SQLStatement        returningStatement =x.getReturningStatement();
  }

  @Override
  public boolean visit(Entry x) {
     final List<SQLName> columns = x.getColumns();
     SQLSelect           subQuery =x.getSubQuery();
     SQLStatement        returningStatement =x.getReturningStatement();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableAlterColumn x) {
     SQLName             originColumn = x.getOriginColumn();
     SQLColumnDefinition column = x.getColumn();
     boolean             setNotNull =x.isSetNotNull();
     boolean             dropNotNull =x.isDropNotNull();
     SQLExpr             setDefault =x.getSetDefault();
     boolean             dropDefault =x.isDropDefault();
     SQLDataType         dataType =x.getDataType();

  }

  @Override
  public boolean visit(SQLAlterTableAlterColumn x) {
     SQLName             originColumn = x.getOriginColumn();
     SQLColumnDefinition column = x.getColumn();
     boolean             setNotNull =x.isSetNotNull();
     boolean             dropNotNull =x.isDropNotNull();
     SQLExpr             setDefault =x.getSetDefault();
     boolean             dropDefault =x.isDropDefault();
     SQLDataType         dataType =x.getDataType();
    return false;
  }

  @Override
  public boolean visit(SQLCheck x) {
     SQLExpr expr = x.getExpr();

    return false;
  }

  @Override
  public void endVisit(SQLCheck x) {
     SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(SQLAlterTableDropForeignKey x) {
     SQLName indexName = x.getIndexName();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropForeignKey x) {
     SQLName indexName = x.getIndexName();
  }

  @Override
  public boolean visit(SQLAlterTableDropPrimaryKey x) {
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropPrimaryKey x) {

  }

  @Override
  public boolean visit(SQLAlterTableDisableKeys x) {
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDisableKeys x) {

  }

  @Override
  public boolean visit(SQLAlterTableEnableKeys x) {
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableEnableKeys x) {

  }

  @Override
  public boolean visit(SQLAlterTableStatement x) {
     SQLExprTableSource      tableSource = x.getTableSource();
     List<SQLAlterTableItem> items                   = x.getItems();

    // for mysql
     boolean                 ignore                  = x.isIgnore();

     boolean                 updateGlobalIndexes     = x.isUpdateGlobalIndexes();
     boolean                 invalidateGlobalIndexes = x.isInvalidateGlobalIndexes();

     boolean                 removePatiting          = x.isRemovePatiting();
     boolean                 upgradePatiting         = x.isUpgradePatiting();
     Map<String, SQLObject>  tableOptions            = x.getTableOptions();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableStatement x) {
     SQLExprTableSource      tableSource = x.getTableSource();
     List<SQLAlterTableItem> items                   = x.getItems();

    // for mysql
     boolean                 ignore                  = x.isIgnore();

     boolean                 updateGlobalIndexes     = x.isUpdateGlobalIndexes();
     boolean                 invalidateGlobalIndexes = x.isInvalidateGlobalIndexes();

     boolean                 removePatiting          = x.isRemovePatiting();
     boolean                 upgradePatiting         = x.isUpgradePatiting();
     Map<String, SQLObject>  tableOptions            = x.getTableOptions();
  }

  @Override
  public boolean visit(SQLAlterTableDisableConstraint x) {
    SQLName constraintName = x.getConstraintName();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDisableConstraint x) {
    SQLName constraintName = x.getConstraintName();
  }

  @Override
  public boolean visit(SQLAlterTableEnableConstraint x) {
    SQLName constraintName = x.getConstraintName();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableEnableConstraint x) {
    SQLName constraintName = x.getConstraintName();
  }

  @Override
  public boolean visit(SQLColumnCheck x) {
     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLColumnCheck x) {
     SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(SQLExprHint x) {
     SQLExpr expr =x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLExprHint x) {
     SQLExpr expr =x.getExpr();
  }

  @Override
  public boolean visit(SQLAlterTableDropConstraint x) {
     SQLName constraintName = x.getConstraintName();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropConstraint x) {
     SQLName constraintName = x.getConstraintName();
  }

  @Override
  public boolean visit(SQLUnique x) {
     final List<SQLSelectOrderByItem> columns = x.getColumns();
    return false;
  }

  @Override
  public void endVisit(SQLUnique x) {
     final List<SQLSelectOrderByItem> columns = x.getColumns();

  }

  @Override
  public boolean visit(SQLPrimaryKeyImpl x) {
    return false;
  }

  @Override
  public void endVisit(SQLPrimaryKeyImpl x) {

  }

  @Override
  public boolean visit(SQLCreateIndexStatement x) {
     SQLName                    name = x.getName();

     SQLTableSource             table =x.getTable();

     List<SQLSelectOrderByItem> items = x.getItems();

     String                     type = x.getType();

    // for mysql
     String                     using =x.getUsing();

     SQLExpr                    comment =x.getComment();
    return false;
  }

  @Override
  public void endVisit(SQLCreateIndexStatement x) {
     SQLName                    name = x.getName();

     SQLTableSource             table =x.getTable();

     List<SQLSelectOrderByItem> items = x.getItems();

     String                     type = x.getType();

    // for mysql
     String                     using =x.getUsing();

     SQLExpr                    comment =x.getComment();
  }

  @Override
  public boolean visit(SQLAlterTableRenameColumn x) {
     SQLName column = x.getColumn();
     SQLName to =x.getTo();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableRenameColumn x) {
     SQLName column = x.getColumn();
     SQLName to =x.getTo();

  }

  @Override
  public boolean visit(SQLColumnReference x) {
     SQLName       table = x.getTable();
     List<SQLName> columns = x.getColumns();

     SQLForeignKeyImpl.Match referenceMatch = x.getReferenceMatch();
     SQLForeignKeyImpl.Option onUpdate = x.getOnUpdate();
     SQLForeignKeyImpl.Option onDelete =x.getOnDelete();
    return false;
  }

  @Override
  public void endVisit(SQLColumnReference x) {
     SQLName       table = x.getTable();
     List<SQLName> columns = x.getColumns();

     SQLForeignKeyImpl.Match referenceMatch = x.getReferenceMatch();
     SQLForeignKeyImpl.Option onUpdate = x.getOnUpdate();
     SQLForeignKeyImpl.Option onDelete =x.getOnDelete();
  }

  @Override
  public boolean visit(SQLForeignKeyImpl x) {
     SQLExprTableSource referencedTable = x.getReferencedTable();
     List<SQLName>      referencingColumns = x.getReferencingColumns();
     List<SQLName>      referencedColumns  = x.getReferencedColumns();
     boolean            onDeleteCascade    = x.isOnDeleteCascade();
     boolean            onDeleteSetNull    = x.isOnDeleteSetNull();
    return false;
  }

  @Override
  public void endVisit(SQLForeignKeyImpl x) {
     SQLExprTableSource referencedTable = x.getReferencedTable();
     List<SQLName>      referencingColumns = x.getReferencingColumns();
     List<SQLName>      referencedColumns  = x.getReferencedColumns();
     boolean            onDeleteCascade    = x.isOnDeleteCascade();
     boolean            onDeleteSetNull    = x.isOnDeleteSetNull();
  }

  @Override
  public boolean visit(SQLDropSequenceStatement x) {
     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();
    return false;
  }

  @Override
  public void endVisit(SQLDropSequenceStatement x) {
     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();
  }

  @Override
  public boolean visit(SQLDropTriggerStatement x) {

     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();
    return false;
  }

  @Override
  public void endVisit(SQLDropTriggerStatement x) {

     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();

  }

  @Override
  public void endVisit(SQLDropUserStatement x) {
     List<SQLExpr> users = x.getUsers();

  }

  @Override
  public boolean visit(SQLDropUserStatement x) {
     List<SQLExpr> users = x.getUsers();
    return false;
  }

  @Override
  public void endVisit(SQLExplainStatement x) {
     String type = x.getType();
     SQLStatement       statement = x.getStatement();
     List<SQLCommentHint> hints =x.getHints();
  }

  @Override
  public boolean visit(SQLExplainStatement x) {
     String type = x.getType();
     SQLStatement       statement = x.getStatement();
     List<SQLCommentHint> hints =x.getHints();

    return false;
  }

  @Override
  public void endVisit(SQLGrantStatement x) {
     final List<SQLExpr> privileges = x.getPrivileges();

     SQLObject           on = x.getOn();
     SQLExpr             to =x.getTo();
  }

  @Override
  public boolean visit(SQLGrantStatement x) {

     final List<SQLExpr> privileges = x.getPrivileges();

     SQLObject           on = x.getOn();
     SQLExpr             to =x.getTo();

    return false;
  }

  @Override
  public void endVisit(SQLDropDatabaseStatement x) {
     SQLExpr database = x.getDatabase();
     boolean ifExists = x.isIfExists();
  }

  @Override
  public boolean visit(SQLDropDatabaseStatement x) {
     SQLExpr database = x.getDatabase();
     boolean ifExists = x.isIfExists();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableAddIndex x) {
     boolean                          unique = x.isUnique();

     SQLName                          name =x.getName();

     final List<SQLSelectOrderByItem> items = x.getItems();

     String                           type = x.getType();

     String                           using = x.getUsing();

     boolean                          key = x.isKey();

     SQLExpr                        comment = x.getComment();
  }

  @Override
  public boolean visit(SQLAlterTableAddIndex x) {
     boolean                          unique = x.isUnique();

     SQLName                          name =x.getName();

     final List<SQLSelectOrderByItem> items = x.getItems();

     String                           type = x.getType();

     String                           using = x.getUsing();

     boolean                          key = x.isKey();

     SQLExpr                        comment = x.getComment();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableAddConstraint x) {
     SQLConstraint constraint = x.getConstraint();
     boolean      withNoCheck = x.isWithNoCheck();
  }

  @Override
  public boolean visit(SQLAlterTableAddConstraint x) {
     SQLConstraint constraint = x.getConstraint();
     boolean      withNoCheck = x.isWithNoCheck();
    return false;
  }

  @Override
  public void endVisit(SQLCreateTriggerStatement x) {
     SQLName                  name = x.getName();
     boolean                  orReplace      = x.isOrReplace();
     TriggerType triggerType = x.getTriggerType();

     SQLName                  definer =x.getDefiner();

     boolean                  update =x.isUpdate();
     boolean                  delete = x.isDelete();
     boolean                  insert = x.isInsert();

     SQLExprTableSource       on =x.getOn();

     boolean                  forEachRow     = x.isForEachRow();

     List<SQLName>            updateOfColumns = x.getUpdateOfColumns();

     SQLExpr                  when =x.getWhen();
     SQLStatement             body =x.getBody();
  }

  @Override
  public boolean visit(SQLCreateTriggerStatement x) {

     SQLName                  name = x.getName();
     boolean                  orReplace      = x.isOrReplace();
     TriggerType triggerType = x.getTriggerType();

     SQLName                  definer =x.getDefiner();

     boolean                  update =x.isUpdate();
     boolean                  delete = x.isDelete();
     boolean                  insert = x.isInsert();

     SQLExprTableSource       on =x.getOn();

     boolean                  forEachRow     = x.isForEachRow();

     List<SQLName>            updateOfColumns = x.getUpdateOfColumns();

     SQLExpr                  when =x.getWhen();
     SQLStatement             body =x.getBody();
    return false;
  }

  @Override
  public void endVisit(SQLDropFunctionStatement x) {
     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();
  }

  @Override
  public boolean visit(SQLDropFunctionStatement x) {
     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();

    return false;
  }

  @Override
  public void endVisit(SQLDropTableSpaceStatement x) {
     SQLName name =x.getName();
     boolean ifExists =x.isIfExists();
     SQLExpr engine =x.getEngine();
  }

  @Override
  public boolean visit(SQLDropTableSpaceStatement x) {

     SQLName name =x.getName();
     boolean ifExists =x.isIfExists();
     SQLExpr engine =x.getEngine();
    return false;
  }

  @Override
  public void endVisit(SQLDropProcedureStatement x) {
     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();
  }

  @Override
  public boolean visit(SQLDropProcedureStatement x) {

     SQLName name = x.getName();
     boolean ifExists =x.isIfExists();

    return false;
  }

  @Override
  public void endVisit(SQLBooleanExpr x) {
    boolean booleanValue = x.getBooleanValue();

  }

  @Override
  public boolean visit(SQLBooleanExpr x) {
    boolean booleanValue = x.getBooleanValue();
    return false;
  }

  @Override
  public void endVisit(SQLUnionQueryTableSource x) {
     SQLUnionQuery union = x.getUnion();
  }

  @Override
  public boolean visit(SQLUnionQueryTableSource x) {

     SQLUnionQuery union = x.getUnion();

    return false;
  }

  @Override
  public void endVisit(SQLTimestampExpr x) {
     String  literal =x.getLiteral();
     String  timeZone =x.getTimeZone();
     boolean withTimeZone = x.isWithTimeZone();
  }

  @Override
  public boolean visit(SQLTimestampExpr x) {
     String  literal =x.getLiteral();
     String  timeZone =x.getTimeZone();
     boolean withTimeZone = x.isWithTimeZone();


    return false;
  }

  @Override
  public void endVisit(SQLRevokeStatement x) {

     final List<SQLExpr> privileges = x.getPrivileges();

     SQLObject           on =x.getOn();
     SQLExpr             from =x.getFrom();
    // mysql
     SQLObjectType       objectType =x.getObjectType();
  }

  @Override
  public boolean visit(SQLRevokeStatement x) {

     final List<SQLExpr> privileges = x.getPrivileges();

     SQLObject           on =x.getOn();
     SQLExpr             from =x.getFrom();
    // mysql
     SQLObjectType       objectType =x.getObjectType();
    return false;
  }

  @Override
  public void endVisit(SQLBinaryExpr x) {
     String text = x.getText();

      Number val =x.getValue();
  }

  @Override
  public boolean visit(SQLBinaryExpr x) {
     String text = x.getText();

      Number val =x.getValue();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableRename x) {
     SQLExprTableSource to = x.getTo();

  }

  @Override
  public boolean visit(SQLAlterTableRename x) {
     SQLExprTableSource to = x.getTo();
    return false;
  }

  @Override
  public void endVisit(SQLAlterViewRenameStatement x) {
     SQLName name = x.getName();
     SQLName to =x.getTo();
  }

  @Override
  public boolean visit(SQLAlterViewRenameStatement x) {
     SQLName name = x.getName();
     SQLName to =x.getTo();
    return false;
  }

  @Override
  public void endVisit(SQLShowTablesStatement x) {
     SQLName database = x.getDatabase();
     SQLExpr like =x.getLike();

    // for mysql
     boolean full =x.isFull();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(SQLShowTablesStatement x) {
     SQLName database = x.getDatabase();
     SQLExpr like =x.getLike();

    // for mysql
     boolean full =x.isFull();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableAddPartition x) {
     boolean               ifNotExists = x.isIfNotExists();

     final List<SQLObject> partitions  = x.getPartitions();

     SQLExpr               partitionCount =x.getPartitionCount();
  }

  @Override
  public boolean visit(SQLAlterTableAddPartition x) {
     boolean               ifNotExists = x.isIfNotExists();

     final List<SQLObject> partitions  = x.getPartitions();

     SQLExpr               partitionCount =x.getPartitionCount();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropPartition x) {
     boolean ifExists = x.isIfExists();

     boolean purge = x.isPurge();

     final List<SQLObject> partitions = x.getPartitions();
  }

  @Override
  public boolean visit(SQLAlterTableDropPartition x) {
     boolean ifExists = x.isIfExists();

     boolean purge = x.isPurge();

     final List<SQLObject> partitions = x.getPartitions();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableRenamePartition x) {
     boolean ifNotExists = x.isIfNotExists();

     final List<SQLAssignItem> partition = x.getPartition();
     final List<SQLAssignItem> to = x.getTo();
  }

  @Override
  public boolean visit(SQLAlterTableRenamePartition x) {
     boolean ifNotExists = x.isIfNotExists();

     final List<SQLAssignItem> partition = x.getPartition();
     final List<SQLAssignItem> to = x.getTo();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableSetComment x) {
     SQLExpr lifecycle = x.getComment();
  }

  @Override
  public boolean visit(SQLAlterTableSetComment x) {
     SQLExpr lifecycle = x.getComment();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableSetLifecycle x) {
     SQLExpr lifecycle = x.getLifecycle();

  }

  @Override
  public boolean visit(SQLAlterTableSetLifecycle x) {
     SQLExpr lifecycle = x.getLifecycle();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableEnableLifecycle x) {
     final List<SQLAssignItem> partition = x.getPartition();
  }

  @Override
  public boolean visit(SQLAlterTableEnableLifecycle x) {
     final List<SQLAssignItem> partition = x.getPartition();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDisableLifecycle x) {
     final List<SQLAssignItem> partition = x.getPartition();
  }

  @Override
  public boolean visit(SQLAlterTableDisableLifecycle x) {
     final List<SQLAssignItem> partition = x.getPartition();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableTouch x) {
     final List<SQLAssignItem> partition = x.getPartition();
  }

  @Override
  public boolean visit(SQLAlterTableTouch x) {

     final List<SQLAssignItem> partition = x.getPartition();
    return false;
  }

  @Override
  public void endVisit(SQLArrayExpr x) {
     SQLExpr expr = x.getExpr();

     List<SQLExpr> values = x.getValues();
  }

  @Override
  public boolean visit(SQLArrayExpr x) {
     SQLExpr expr = x.getExpr();

     List<SQLExpr> values = x.getValues();
    return false;
  }

  @Override
  public void endVisit(SQLOpenStatement x) {
    //cursor name
     SQLName cursorName = x.getCursorName();

     final List<SQLName> columns = x.getColumns();

     SQLExpr forExpr = x.getFor();
  }

  @Override
  public boolean visit(SQLOpenStatement x) {
    //cursor name
     SQLName cursorName = x.getCursorName();

     final List<SQLName> columns = x.getColumns();

     SQLExpr forExpr = x.getFor();
    return false;
  }

  @Override
  public void endVisit(SQLFetchStatement x) {
     SQLName cursorName = x.getCursorName();

     boolean bulkCollect = x.isBulkCollect();

     List<SQLExpr> into = x.getInto();
  }

  @Override
  public boolean visit(SQLFetchStatement x) {
     SQLName cursorName = x.getCursorName();

     boolean bulkCollect = x.isBulkCollect();

     List<SQLExpr> into = x.getInto();
    return false;
  }

  @Override
  public void endVisit(SQLCloseStatement x) {
     SQLName cursorName = x.getCursorName();
  }

  @Override
  public boolean visit(SQLCloseStatement x) {
    //cursor name
     SQLName cursorName = x.getCursorName();

    return false;
  }

  @Override
  public boolean visit(SQLGroupingSetExpr x) {
     final List<SQLExpr> parameters = x.getParameters();
    return false;
  }

  @Override
  public void endVisit(SQLGroupingSetExpr x) {

     final List<SQLExpr> parameters = x.getParameters();

  }

  @Override
  public boolean visit(SQLIfStatement x) {
     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
     List<ElseIf> elseIfList = x.getElseIfList();
     Else elseItem = x.getElseItem();

    return false;
  }

  @Override
  public void endVisit(SQLIfStatement x) {

     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
     List<ElseIf> elseIfList = x.getElseIfList();
     Else elseItem = x.getElseItem();


  }

  @Override
  public boolean visit(ElseIf x) {

     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
    return false;
  }

  @Override
  public void endVisit(ElseIf x) {

     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();

  }

  @Override
  public boolean visit(Else x) {
     List<SQLStatement> statements = x.getStatements();
    return false;
  }

  @Override
  public void endVisit(Else x) {
     List<SQLStatement> statements = x.getStatements();

  }

  @Override
  public boolean visit(SQLLoopStatement x) {
     String labelName = x.getLabelName();

     final List<SQLStatement> statements = x.getStatements();
    return false;
  }

  @Override
  public void endVisit(SQLLoopStatement x) {

     String labelName = x.getLabelName();

     final List<SQLStatement> statements = x.getStatements();
  }

  @Override
  public boolean visit(SQLParameter x) {
     SQLName name = x.getName();
     SQLDataType dataType = x.getDataType();
     SQLExpr defaultValue = x.getDefaultValue();
     ParameterType paramType;
     boolean noCopy = x.isNoCopy();
     boolean constant = x.isConstant();
     SQLName cursorName = x.getCursorName();
     final List<SQLParameter> cursorParameters = x.getCursorParameters();
     boolean order = x.isOrder();
     boolean map = x.isMap();
     boolean member = x.isMember();
    return false;
  }

  @Override
  public void endVisit(SQLParameter x) {
     SQLName name = x.getName();
     SQLDataType dataType = x.getDataType();
     SQLExpr defaultValue = x.getDefaultValue();
     ParameterType paramType;
     boolean noCopy = x.isNoCopy();
     boolean constant = x.isConstant();
     SQLName cursorName = x.getCursorName();
     final List<SQLParameter> cursorParameters = x.getCursorParameters();
     boolean order = x.isOrder();
     boolean map = x.isMap();
     boolean member = x.isMember();
  }

  @Override
  public boolean visit(SQLCreateProcedureStatement x) {
     SQLName definer = x.getDefiner();

     boolean create = x.isCreate();
     boolean orReplace = x.isOrReplace();
     SQLName name = x.getName();
     SQLStatement block = x.getBlock();
     List<SQLParameter> parameters = x.getParameters();

     SQLName authid = x.getAuthid();

    // for mysql
     boolean deterministic = x.isDeterministic();
     boolean containsSql = x.isContainsSql();
     boolean noSql = x.isNoSql();
     boolean readSqlData = x.isReadSqlData();
     boolean modifiesSqlData = x.isModifiesSqlData();

     String wrappedSource = x.getWrappedSource();
    return false;
  }

  @Override
  public void endVisit(SQLCreateProcedureStatement x) {
     SQLName definer = x.getDefiner();

     boolean create = x.isCreate();
     boolean orReplace = x.isOrReplace();
     SQLName name = x.getName();
     SQLStatement block = x.getBlock();
     List<SQLParameter> parameters = x.getParameters();

     SQLName authid = x.getAuthid();

    // for mysql
     boolean deterministic = x.isDeterministic();
     boolean containsSql = x.isContainsSql();
     boolean noSql = x.isNoSql();
     boolean readSqlData = x.isReadSqlData();
     boolean modifiesSqlData = x.isModifiesSqlData();

     String wrappedSource = x.getWrappedSource();
  }

  @Override
  public boolean visit(SQLCreateFunctionStatement x) {
     SQLName definer = x.getDefiner();

     boolean create = x.isCreate();
     boolean orReplace = x.isOrReplace();
     SQLName name = x.getName();
     SQLStatement block = x.getBlock();
     List<SQLParameter> parameters = x.getParameters();

    // for mysql

     String comment = x.getComment();
     boolean deterministic = x.isDeterministic();
     boolean parallelEnable = x.isParallelEnable();
     boolean aggregate = x.isAggregate();
     SQLName using = x.getUsing();
     boolean pipelined = x.isPipelined();
     boolean resultCache = x.isResultCache();
     String wrappedSource = x.getWrappedSource();
    return false;
  }

  @Override
  public void endVisit(SQLCreateFunctionStatement x) {
     SQLName definer = x.getDefiner();

     boolean create = x.isCreate();
     boolean orReplace = x.isOrReplace();
     SQLName name = x.getName();
     SQLStatement block = x.getBlock();
     List<SQLParameter> parameters = x.getParameters();

    // for mysql

     String comment = x.getComment();
     boolean deterministic = x.isDeterministic();
     boolean parallelEnable = x.isParallelEnable();
     boolean aggregate = x.isAggregate();
     SQLName using = x.getUsing();
     boolean pipelined = x.isPipelined();
     boolean resultCache = x.isResultCache();
     String wrappedSource = x.getWrappedSource();
  }

  @Override
  public boolean visit(SQLBlockStatement x) {
     String labelName = x.getLabelName();
     String endLabel = x.getEndLabel();
     List<SQLParameter> parameters = x.getParameters();
     List<SQLStatement> statementList = x.getStatementList();
     SQLStatement exception = x.getException();
     boolean endOfCommit = x.isEndOfCommit();
    return false;
  }

  @Override
  public void endVisit(SQLBlockStatement x) {
     String labelName = x.getLabelName();
     String endLabel = x.getEndLabel();
     List<SQLParameter> parameters = x.getParameters();
     List<SQLStatement> statementList = x.getStatementList();
     SQLStatement exception = x.getException();
     boolean endOfCommit = x.isEndOfCommit();
  }

  @Override
  public boolean visit(SQLAlterTableDropKey x) {
     SQLName keyName = x.getKeyName();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDropKey x) {
     SQLName keyName = x.getKeyName();
  }

  @Override
  public boolean visit(SQLDeclareItem x) {
     SQLDeclareItem.Type type = x.getType();

     SQLName name = x.getName();

     SQLDataType dataType = x.getDataType();

     SQLExpr value = x.getValue();

     List<SQLTableElement> tableElementList = x.getTableElementList();

      SQLObject resolvedObject = x.getResolvedObject();
    return false;
  }

  @Override
  public void endVisit(SQLDeclareItem x) {
     SQLDeclareItem.Type type = x.getType();

     SQLName name = x.getName();

     SQLDataType dataType = x.getDataType();

     SQLExpr value = x.getValue();

     List<SQLTableElement> tableElementList = x.getTableElementList();

      SQLObject resolvedObject = x.getResolvedObject();
  }

  @Override
  public boolean visit(SQLPartitionValue x) {

     Operator operator = x.getOperator();
     final List<SQLExpr> items = x.getItems();
    return false;
  }

  @Override
  public void endVisit(SQLPartitionValue x) {

     Operator operator = x.getOperator();
     final List<SQLExpr> items = x.getItems();
  }

  @Override
  public boolean visit(SQLPartition x) {
     SQLName name = x.getName();

     SQLExpr subPartitionsCount = x.getSubPartitionsCount();

     List<SQLSubPartition> subPartitions = x.getSubPartitions();

     SQLPartitionValue values = x.getValues();

    // for mysql
     SQLExpr dataDirectory = x.getDataDirectory();
     SQLExpr indexDirectory = x.getIndexDirectory();
     SQLExpr maxRows = x.getMaxRows();
     SQLExpr minRows = x.getMinRows();
     SQLExpr engine = x.getEngine();
     SQLExpr comment = x.getComment();

    return false;
  }

  @Override
  public void endVisit(SQLPartition x) {
     SQLName name = x.getName();

     SQLExpr subPartitionsCount = x.getSubPartitionsCount();

     List<SQLSubPartition> subPartitions = x.getSubPartitions();

     SQLPartitionValue values = x.getValues();

    // for mysql
     SQLExpr dataDirectory = x.getDataDirectory();
     SQLExpr indexDirectory = x.getIndexDirectory();
     SQLExpr maxRows = x.getMaxRows();
     SQLExpr minRows = x.getMinRows();
     SQLExpr engine = x.getEngine();
     SQLExpr comment = x.getComment();

  }

  @Override
  public boolean visit(SQLPartitionByRange x) {
     SQLExpr interval = x.getInterval();

    return false;
  }

  @Override
  public void endVisit(SQLPartitionByRange x) {
     SQLExpr interval = x.getInterval();
  }

  @Override
  public boolean visit(SQLPartitionByHash x) {
    //null

    return false;
  }

  @Override
  public void endVisit(SQLPartitionByHash x) {
    //null
  }

  @Override
  public boolean visit(SQLPartitionByList x) {
    //null
    return false;
  }

  @Override
  public void endVisit(SQLPartitionByList x) {
    //null
  }

  @Override
  public boolean visit(SQLSubPartition x) {
     SQLName name = x.getName();
     SQLPartitionValue values = x.getValues();
     SQLName tableSpace = x.getTableSpace();
    return false;
  }

  @Override
  public void endVisit(SQLSubPartition x) {
     SQLName name = x.getName();
     SQLPartitionValue values = x.getValues();
     SQLName tableSpace = x.getTableSpace();
  }

  @Override
  public boolean visit(SQLSubPartitionByHash x) {
     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLSubPartitionByHash x) {
     SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(SQLSubPartitionByList x) {
     SQLName column = x.getColumn();
    return false;
  }

  @Override
  public void endVisit(SQLSubPartitionByList x) {
     SQLName column = x.getColumn();
  }

  @Override
  public boolean visit(SQLAlterDatabaseStatement x) {
     SQLName name = x.getName();

     boolean upgradeDataDirectoryName = x.isUpgradeDataDirectoryName();

     SQLAlterCharacter character = x.getCharacter();
    return false;
  }

  @Override
  public void endVisit(SQLAlterDatabaseStatement x) {
     SQLName name = x.getName();

     boolean upgradeDataDirectoryName = x.isUpgradeDataDirectoryName();

     SQLAlterCharacter character = x.getCharacter();
  }

  @Override
  public boolean visit(SQLAlterTableConvertCharSet x) {
     SQLExpr charset = x.getCharset();
     SQLExpr collate = x.getCollate();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableConvertCharSet x) {
     SQLExpr charset = x.getCharset();
     SQLExpr collate = x.getCollate();
  }

  @Override
  public boolean visit(SQLAlterTableReOrganizePartition x) {
     final List<SQLName> names = x.getNames();

     final List<SQLObject> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableReOrganizePartition x) {
     final List<SQLName> names = x.getNames();

     final List<SQLObject> partitions = x.getPartitions();

  }

  @Override
  public boolean visit(SQLAlterTableCoalescePartition x) {
     SQLExpr count = x.getCount();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableCoalescePartition x) {
     SQLExpr count = x.getCount();
  }

  @Override
  public boolean visit(SQLAlterTableTruncatePartition x) {
     final List<SQLName> partitions = x.getPartitions();

    return false;
  }

  @Override
  public void endVisit(SQLAlterTableTruncatePartition x) {
     final List<SQLName> partitions = x.getPartitions();

  }

  @Override
  public boolean visit(SQLAlterTableDiscardPartition x) {
     final List<SQLName> partitions = x.getPartitions();
     boolean tablespace = x.isTablespace();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableDiscardPartition x) {
     final List<SQLName> partitions = x.getPartitions();
     boolean tablespace = x.isTablespace();

  }

  @Override
  public boolean visit(SQLAlterTableImportPartition x) {
     final List<SQLName> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableImportPartition x) {
     final List<SQLName> partitions = x.getPartitions();
  }

  @Override
  public boolean visit(SQLAlterTableAnalyzePartition x) {
     final List<SQLName> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableAnalyzePartition x) {
     final List<SQLName> partitions = x.getPartitions();
  }

  @Override
  public boolean visit(SQLAlterTableCheckPartition x) {
     final List<SQLName> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableCheckPartition x) {
     final List<SQLName> partitions = x.getPartitions();
  }

  @Override
  public boolean visit(SQLAlterTableOptimizePartition x) {
     final List<SQLName> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableOptimizePartition x) {
     final List<SQLName> partitions = x.getPartitions();
  }

  @Override
  public boolean visit(SQLAlterTableRebuildPartition x) {
     final List<SQLName> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableRebuildPartition x) {
     final List<SQLName> partitions = x.getPartitions();
  }

  @Override
  public boolean visit(SQLAlterTableRepairPartition x) {
     final List<SQLName> partitions = x.getPartitions();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTableRepairPartition x) {

     final List<SQLName> partitions = x.getPartitions();

  }

  @Override
  public boolean visit(SQLSequenceExpr x) {
     SQLName sequence = x.getSequence();
     Function function = x.getFunction();
    return false;
  }

  @Override
  public void endVisit(SQLSequenceExpr x) {
     SQLName sequence = x.getSequence();
     Function function = x.getFunction();

  }

  @Override
  public boolean visit(SQLMergeStatement x) {
     final List<SQLHint> hints = x.getHints();
     SQLTableSource into = x.getInto();
     String alias = x.getAlias();
     SQLTableSource using = x.getUsing();
     SQLExpr on = x.getOn();
     MergeUpdateClause updateClause = x.getUpdateClause();
     MergeInsertClause insertClause = x.getInsertClause();
     SQLErrorLoggingClause errorLoggingClause = x.getErrorLoggingClause();
    return false;
  }

  @Override
  public void endVisit(SQLMergeStatement x) {
     final List<SQLHint> hints = x.getHints();
     SQLTableSource into = x.getInto();
     String alias = x.getAlias();
     SQLTableSource using = x.getUsing();
     SQLExpr on = x.getOn();
     MergeUpdateClause updateClause = x.getUpdateClause();
     MergeInsertClause insertClause = x.getInsertClause();
     SQLErrorLoggingClause errorLoggingClause = x.getErrorLoggingClause();
  }

  @Override
  public boolean visit(MergeUpdateClause x) {
     List<SQLUpdateSetItem> items = x.getItems();
     SQLExpr where = x.getWhere();
     SQLExpr deleteWhere = x.getDeleteWhere();
    return false;
  }

  @Override
  public void endVisit(MergeUpdateClause x) {
     List<SQLUpdateSetItem> items = x.getItems();
     SQLExpr where = x.getWhere();
     SQLExpr deleteWhere = x.getDeleteWhere();
  }

  @Override
  public boolean visit(MergeInsertClause x) {
     List<SQLExpr> columns = x.getColumns();
     List<SQLExpr> values = x.getValues();
     SQLExpr where = x.getWhere();
    return false;
  }

  @Override
  public void endVisit(MergeInsertClause x) {
     List<SQLExpr> columns = x.getColumns();
     List<SQLExpr> values = x.getValues();
     SQLExpr where = x.getWhere();
  }

  @Override
  public boolean visit(SQLErrorLoggingClause x) {

     SQLName into = x.getInto();
     SQLExpr simpleExpression = x.getSimpleExpression();
     SQLExpr limit = x.getLimit();
    return false;
  }

  @Override
  public void endVisit(SQLErrorLoggingClause x) {

     SQLName into = x.getInto();
     SQLExpr simpleExpression = x.getSimpleExpression();
     SQLExpr limit = x.getLimit();

  }

  @Override
  public boolean visit(SQLNullConstraint x) {
    //null

    return false;
  }

  @Override
  public void endVisit(SQLNullConstraint x) {
//null
  }

  @Override
  public boolean visit(SQLCreateSequenceStatement x) {
     SQLName name = x.getName();

     SQLExpr startWith = x.getStartWith();
     SQLExpr incrementBy = x.getIncrementBy();
     SQLExpr minValue = x.getMinValue();
     SQLExpr maxValue = x.getMaxValue();
     boolean noMaxValue = x.isNoMaxValue();
     boolean noMinValue = x.isNoMinValue();

     Boolean cycle = x.getCycle();
     Boolean cache = x.getCache();
     SQLExpr cacheValue = x.getCacheValue();

     Boolean order = x.getOrder();
    return false;
  }

  @Override
  public void endVisit(SQLCreateSequenceStatement x) {
     SQLName name = x.getName();

     SQLExpr startWith = x.getStartWith();
     SQLExpr incrementBy = x.getIncrementBy();
     SQLExpr minValue = x.getMinValue();
     SQLExpr maxValue = x.getMaxValue();
     boolean noMaxValue = x.isNoMaxValue();
     boolean noMinValue = x.isNoMinValue();

     Boolean cycle = x.getCycle();
     Boolean cache = x.getCache();
     SQLExpr cacheValue = x.getCacheValue();

     Boolean order = x.getOrder();
  }

  @Override
  public boolean visit(SQLDateExpr x) {
     SQLExpr literal = x.getLiteral();
    return false;
  }

  @Override
  public void endVisit(SQLDateExpr x) {
     SQLExpr literal = x.getLiteral();
  }

  @Override
  public boolean visit(SQLLimit x) {

     SQLExpr rowCount = x.getRowCount();
     SQLExpr offset = x.getOffset();
    return false;
  }

  @Override
  public void endVisit(SQLLimit x) {

     SQLExpr rowCount = x.getRowCount();
     SQLExpr offset = x.getOffset();
  }

  @Override
  public void endVisit(SQLStartTransactionStatement x) {
     boolean consistentSnapshot = x.isConsistentSnapshot();

     boolean begin = x.isBegin();
     boolean work = x.isWork();
     SQLExpr name = x.getName();

     List<SQLCommentHint> hints = x.getHints();
  }

  @Override
  public boolean visit(SQLStartTransactionStatement x) {
     boolean consistentSnapshot = x.isConsistentSnapshot();

     boolean begin = x.isBegin();
     boolean work = x.isWork();
     SQLExpr name = x.getName();

     List<SQLCommentHint> hints = x.getHints();
    return false;
  }

  @Override
  public void endVisit(SQLDescribeStatement x) {
     SQLName object = x.getObject();

     SQLName column = x.getColumn();

    // for odps
     SQLObjectType objectType = x.getObjectType();
     List<SQLExpr> partition = x.getPartition();
  }

  @Override
  public boolean visit(SQLDescribeStatement x) {
     SQLName object = x.getObject();

     SQLName column = x.getColumn();

    // for odps
     SQLObjectType objectType = x.getObjectType();
     List<SQLExpr> partition = x.getPartition();

    return false;
  }

  @Override
  public boolean visit(SQLWhileStatement x) {
    //while expr
     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
    //while label name
     String labelName = x.getLabelName();
    return false;
  }

  @Override
  public void endVisit(SQLWhileStatement x) {
    //while expr
     SQLExpr condition = x.getCondition();
     List<SQLStatement> statements = x.getStatements();
    //while label name
     String labelName = x.getLabelName();
  }

  @Override
  public boolean visit(SQLDeclareStatement x) {

     List<SQLDeclareItem> items = x.getItems();
    return false;
  }

  @Override
  public void endVisit(SQLDeclareStatement x) {

     List<SQLDeclareItem> items = x.getItems();
  }

  @Override
  public boolean visit(SQLReturnStatement x) {

     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLReturnStatement x) {

     SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(SQLArgument x) {
     SQLParameter.ParameterType type = x.getType();
     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLArgument x) {
     SQLParameter.ParameterType type = x.getType();
     SQLExpr expr = x.getExpr();

  }

  @Override
  public boolean visit(SQLCommitStatement x) {
    // mysql
     boolean work = x.isWork();
     Boolean chain = x.getChain();
     Boolean release = x.getRelease();
    return false;
  }

  @Override
  public void endVisit(SQLCommitStatement x) {
    // mysql
     boolean work = x.isWork();
     Boolean chain = x.getChain();
     Boolean release = x.getRelease();
  }

  @Override
  public boolean visit(SQLFlashbackExpr x) {
     SQLFlashbackExpr.Type type = x.getType();
     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLFlashbackExpr x) {
     SQLFlashbackExpr.Type type = x.getType();
     SQLExpr expr = x.getExpr();

  }

  @Override
  public boolean visit(SQLCreateMaterializedViewStatement x) {
     SQLName name = x.getName();
     List<SQLName> columns = x.getColumns();

     boolean refreshFast = x.isRefreshFast();
     boolean refreshComlete = x.isRefreshComlete();
     boolean refreshForce = x.isRefreshForce();
     boolean refreshOnCommit = x.isRefreshOnCommit();
     boolean refreshOnDemand = x.isRefreshOnDemand();

     boolean buildImmediate = x.isBuildImmediate();
     boolean buildDeferred = x.isBuildDeferred();

     SQLSelect query = x.getQuery();

    // oracle
     Integer pctfree = x.getPctfree();
     Integer pctused = x.getPctused();
     Integer initrans = x.getInitrans();

     Integer maxtrans = x.getMaxtrans();
     Integer pctincrease = x.getPctincrease();
     Integer freeLists = x.getFreeLists();
     Boolean compress = x.getCompress();
     Integer compressLevel = x.getCompressLevel();
     boolean compressForOltp = x.isCompressForOltp();
     Integer pctthreshold = x.getPctthreshold();
    return false;
  }

  @Override
  public void endVisit(SQLCreateMaterializedViewStatement x) {
     SQLName name = x.getName();
     List<SQLName> columns = x.getColumns();

     boolean refreshFast = x.isRefreshFast();
     boolean refreshComlete = x.isRefreshComlete();
     boolean refreshForce = x.isRefreshForce();
     boolean refreshOnCommit = x.isRefreshOnCommit();
     boolean refreshOnDemand = x.isRefreshOnDemand();

     boolean buildImmediate = x.isBuildImmediate();
     boolean buildDeferred = x.isBuildDeferred();

     SQLSelect query = x.getQuery();

    // oracle
     Integer pctfree = x.getPctfree();
     Integer pctused = x.getPctused();
     Integer initrans = x.getInitrans();

     Integer maxtrans = x.getMaxtrans();
     Integer pctincrease = x.getPctincrease();
     Integer freeLists = x.getFreeLists();
     Boolean compress = x.getCompress();
     Integer compressLevel = x.getCompressLevel();
     boolean compressForOltp = x.isCompressForOltp();
     Integer pctthreshold = x.getPctthreshold();

     Boolean logging;
     Boolean cache;

     SQLName tablespace;
     SQLObject storage;

     Boolean parallel;
     Integer parallelValue;

     Boolean enableQueryRewrite;

     SQLPartitionBy partitionBy;

     boolean withRowId;
  }

  @Override
  public boolean visit(SQLBinaryOpExprGroup x) {
     final SQLBinaryOperator operator = x.getOperator();
     final List<SQLExpr> items = x.getItems();
    return false;
  }

  @Override
  public void endVisit(SQLBinaryOpExprGroup x) {
     final SQLBinaryOperator operator = x.getOperator();
     final List<SQLExpr> items = x.getItems();

  }

  @Override
  public boolean visit(SQLScriptCommitStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(SQLScriptCommitStatement x) {
//null
  }

  @Override
  public boolean visit(SQLReplaceStatement x) {
     boolean lowPriority = x.isLowPriority();
     boolean delayed = x.isDelayed();

     SQLExprTableSource tableSource = x.getTableSource();
     final List<SQLExpr> columns = x.getColumns();
     List<SQLInsertStatement.ValuesClause> valuesList = x.getValuesList();
     SQLQueryExpr query = x.getQuery();
    return false;
  }

  @Override
  public void endVisit(SQLReplaceStatement x) {
     boolean lowPriority = x.isLowPriority();
     boolean delayed = x.isDelayed();

     SQLExprTableSource tableSource = x.getTableSource();
     final List<SQLExpr> columns = x.getColumns();
     List<SQLInsertStatement.ValuesClause> valuesList = x.getValuesList();
     SQLQueryExpr query = x.getQuery();
  }

  @Override
  public boolean visit(SQLCreateUserStatement x) {
     SQLName user = x.getUser();
     SQLExpr password = x.getPassword();
    return false;
  }

  @Override
  public void endVisit(SQLCreateUserStatement x) {
     SQLName user = x.getUser();
     SQLExpr password = x.getPassword();

    // oracle

  }

  @Override
  public boolean visit(SQLAlterFunctionStatement x) {
     SQLName name = x.getName();

     boolean debug = x.isDebug();
     boolean reuseSettings = x.isReuseSettings();

     SQLExpr comment = x.getComment();
     boolean languageSql = x.isLanguageSql();
     boolean containsSql = x.isContainsSql();
     SQLExpr sqlSecurity = x.getSqlSecurity();
    return false;
  }

  @Override
  public void endVisit(SQLAlterFunctionStatement x) {
     SQLName name = x.getName();

     boolean debug = x.isDebug();
     boolean reuseSettings = x.isReuseSettings();

     SQLExpr comment = x.getComment();
     boolean languageSql = x.isLanguageSql();
     boolean containsSql = x.isContainsSql();
     SQLExpr sqlSecurity = x.getSqlSecurity();
  }

  @Override
  public boolean visit(SQLAlterTypeStatement x) {
     SQLName name = x.getName();

     boolean compile = x.isCompile();
     boolean debug = x.isDebug();
     boolean body = x.isBody();
     boolean reuseSettings = x.isReuseSettings();
    return false;
  }

  @Override
  public void endVisit(SQLAlterTypeStatement x) {
     SQLName name = x.getName();

     boolean compile = x.isCompile();
     boolean debug = x.isDebug();
     boolean body = x.isBody();
     boolean reuseSettings = x.isReuseSettings();
  }

  @Override
  public boolean visit(SQLIntervalExpr x) {
     SQLExpr value = x.getValue();
     SQLIntervalUnit unit = x.getUnit();
    return false;
  }

  @Override
  public void endVisit(SQLIntervalExpr x) {

     SQLExpr value = x.getValue();
     SQLIntervalUnit unit = x.getUnit();
  }

  @Override
  public boolean visit(SQLLateralViewTableSource x) {

     SQLTableSource tableSource = x.getTableSource();

     SQLMethodInvokeExpr method = x.getMethod();

     List<SQLName> columns = x.getColumns();
    return false;
  }

  @Override
  public void endVisit(SQLLateralViewTableSource x) {

     SQLTableSource tableSource = x.getTableSource();

     SQLMethodInvokeExpr method = x.getMethod();

     List<SQLName> columns = x.getColumns();
  }

  @Override
  public boolean visit(SQLShowErrorsStatement x) {
    //null
    return false;
  }

  @Override
  public void endVisit(SQLShowErrorsStatement x) {
//null
  }

  @Override
  public boolean visit(SQLAlterCharacter x) {

     SQLExpr characterSet = x.getCharacterSet();
     SQLExpr collate = x.getCollate();
    return false;
  }

  @Override
  public void endVisit(SQLAlterCharacter x) {

     SQLExpr characterSet = x.getCharacterSet();
     SQLExpr collate = x.getCollate();
  }

  @Override
  public boolean visit(SQLExprStatement x) {
     SQLExpr expr = x.getExpr();
    return false;
  }

  @Override
  public void endVisit(SQLExprStatement x) {
     SQLExpr expr = x.getExpr();
  }

  @Override
  public boolean visit(SQLAlterProcedureStatement x) {
     SQLExpr name = x.getName();

     boolean compile = x.isCompile();
     boolean reuseSettings = x.isReuseSettings();

     SQLExpr comment = x.getComment();
     boolean languageSql = x.isLanguageSql();
     boolean containsSql = x.isContainsSql();
     SQLExpr sqlSecurity = x.getSqlSecurity();
    return false;
}

  @Override
  public void endVisit(SQLAlterProcedureStatement x) {
     SQLExpr name = x.getName();

     boolean compile = x.isCompile();
     boolean reuseSettings = x.isReuseSettings();

     SQLExpr comment = x.getComment();
     boolean languageSql = x.isLanguageSql();
     boolean containsSql = x.isContainsSql();
     SQLExpr sqlSecurity = x.getSqlSecurity();
  }

  @Override
  public boolean visit(SQLAlterViewStatement x) {
       boolean force = x.isForce();
      //  SQLName   name;
       SQLSelect subQuery = x.getSubQuery();
       boolean ifNotExists = x.isIfNotExists();

       String algorithm = x.getAlgorithm();
       SQLName definer = x.getDefiner();
       String sqlSecurity = x.getSqlSecurity();

       SQLExprTableSource tableSource = x.getTableSource();

       final List<SQLTableElement> columns = x.getColumns();

       boolean withCheckOption = x.isWithCheckOption();
       boolean withCascaded = x.isWithCascaded();
       boolean withLocal = x.isWithLocal();
       boolean withReadOnly = x.isWithReadOnly();

       SQLLiteralExpr comment = x.getComment();
      return false;
    }

    @Override
    public void endVisit (SQLAlterViewStatement x){
       boolean force = x.isForce();
      //  SQLName   name;
       SQLSelect subQuery = x.getSubQuery();
       boolean ifNotExists = x.isIfNotExists();

       String algorithm = x.getAlgorithm();
       SQLName definer = x.getDefiner();
       String sqlSecurity = x.getSqlSecurity();

       SQLExprTableSource tableSource = x.getTableSource();

       final List<SQLTableElement> columns = x.getColumns();

       boolean withCheckOption = x.isWithCheckOption();
       boolean withCascaded = x.isWithCascaded();
       boolean withLocal = x.isWithLocal();
       boolean withReadOnly = x.isWithReadOnly();

       SQLLiteralExpr comment = x.getComment();

    }

    @Override
    public boolean visit (SQLDropEventStatement x){
      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
      return false;
    }

    @Override
    public void endVisit (SQLDropEventStatement x){
      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
    }

    @Override
    public boolean visit (SQLDropLogFileGroupStatement x){
      SQLExpr name = x.getName();
      SQLExpr engine = x.getEngine();
      return false;
    }

    @Override
    public void endVisit (SQLDropLogFileGroupStatement x){
      SQLExpr name = x.getName();
      SQLExpr engine = x.getEngine();
    }

    @Override
    public boolean visit (SQLDropServerStatement x){

      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
      return false;
    }

    @Override
    public void endVisit (SQLDropServerStatement x){

      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
    }

    @Override
    public boolean visit (SQLDropSynonymStatement x){
      SQLName name = x.getName();
      boolean ifExists = x.isIfExists();
      boolean isPublic = x.isPublic();
      boolean force = x.isForce();
      return false;
    }

    @Override
    public void endVisit (SQLDropSynonymStatement x){
      SQLName name = x.getName();
      boolean ifExists = x.isIfExists();
      boolean isPublic = x.isPublic();
      boolean force = x.isForce();
    }

    @Override
    public boolean visit (SQLRecordDataType x){
      List<SQLColumnDefinition> columns = x.getColumns();

      return false;
    }

    @Override
    public void endVisit (SQLRecordDataType x){
       final List<SQLColumnDefinition> columns = x.getColumns();
    }

    @Override
    public boolean visit (SQLDropTypeStatement x){
      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
      return false;
    }

    @Override
    public void endVisit (SQLDropTypeStatement x){
      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
    }

    @Override
    public boolean visit (SQLExternalRecordFormat x){
      SQLExpr delimitedBy = x.getDelimitedBy();
      SQLExpr terminatedBy = x.getTerminatedBy();

      return false;
    }

    @Override
    public void endVisit (SQLExternalRecordFormat x){
      SQLExpr delimitedBy = x.getDelimitedBy();
      SQLExpr terminatedBy = x.getTerminatedBy();

    }

    @Override
    public boolean visit (SQLArrayDataType x){
      String dbType = x.getDbType();
      SQLDataType componentType = x.getComponentType();

      return false;
    }

    @Override
    public void endVisit (SQLArrayDataType x){
      String dbType = x.getDbType();
      SQLDataType componentType = x.getComponentType();

    }

    @Override
    public boolean visit (SQLMapDataType x){
      String dbType = x.getDbType();
      SQLDataType keyType = x.getKeyType();
      SQLDataType valueType = x.getValueType();
      return false;
    }

    @Override
    public void endVisit (SQLMapDataType x){
      String dbType = x.getDbType();
      SQLDataType keyType = x.getKeyType();
      SQLDataType valueType = x.getValueType();
    }

    @Override
    public boolean visit (SQLStructDataType x){
      String dbType = x.getDbType();
      List<Field> fields = x.getFields();
      return false;
    }

    @Override
    public void endVisit (SQLStructDataType x){
      String dbType = x.getDbType();
      List<Field> fields = x.getFields();
    }

    @Override
    public boolean visit (Field x){
      SQLName name = x.getName();
      SQLDataType dataType = x.getDataType();
      return false;
    }

    @Override
    public void endVisit (Field x){
      SQLName name = x.getName();
      SQLDataType dataType = x.getDataType();
    }

    @Override
    public boolean visit (SQLDropMaterializedViewStatement x){
      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
      return false;
    }

    @Override
    public void endVisit (SQLDropMaterializedViewStatement x){
      SQLExpr name = x.getName();
      boolean ifExists = x.isIfExists();
    }

    @Override
    public boolean visit (SQLAlterTableRenameIndex x){
      SQLName name = x.getName();
      SQLName to = x.getTo();

      return false;
    }

    @Override
    public void endVisit (SQLAlterTableRenameIndex x){
      SQLName name = x.getName();
      SQLName to = x.getTo();

    }

    @Override
    public boolean visit (SQLAlterSequenceStatement x){
      SQLName name = x.getName();

      SQLExpr startWith = x.getStartWith();
      SQLExpr incrementBy = x.getIncrementBy();
      SQLExpr minValue = x.getMinValue();
      SQLExpr maxValue = x.getMaxValue();
      boolean noMaxValue = x.isNoMaxValue();
      boolean noMinValue = x.isNoMinValue();

      Boolean cycle = x.getCycle();
      Boolean cache = x.getCache();
      SQLExpr cacheValue = x.getCacheValue();

      Boolean order = x.getOrder();
      return false;
    }

    @Override
    public void endVisit (SQLAlterSequenceStatement x){
      SQLName name = x.getName();

      SQLExpr startWith = x.getStartWith();
      SQLExpr incrementBy = x.getIncrementBy();
      SQLExpr minValue = x.getMinValue();
      SQLExpr maxValue = x.getMaxValue();
      boolean noMaxValue = x.isNoMaxValue();
      boolean noMinValue = x.isNoMinValue();

      Boolean cycle = x.getCycle();
      Boolean cache = x.getCache();
      SQLExpr cacheValue = x.getCacheValue();

      Boolean order = x.getOrder();
    }

    @Override
    public boolean visit (SQLAlterTableExchangePartition x){
      SQLName partition = x.getPartition();
      SQLExprTableSource table = x.getTable();
      Boolean validation  =x.getValidation();
      return false;
    }

    @Override
    public void endVisit (SQLAlterTableExchangePartition x){
      SQLName partition = x.getPartition();
      SQLExprTableSource table = x.getTable();
      Boolean validation  =x.getValidation();

    }

    @Override
    public boolean visit (SQLValuesExpr x){
      List<SQLListExpr> values = x.getValues();
      return false;
    }

    @Override
    public void endVisit (SQLValuesExpr x){
      List<SQLListExpr> values = x.getValues();
    }

    @Override
    public boolean visit (SQLValuesTableSource x){
      List<SQLListExpr> values = x.getValues();
      List<SQLName> columns = x.getColumns();
      return false;
    }

    @Override
    public void endVisit (SQLValuesTableSource x){
      List<SQLListExpr> values = x.getValues();
      List<SQLName> columns = x.getColumns();
    }

    @Override
    public boolean visit (SQLContainsExpr x){
      boolean not = x.isNot();
      SQLExpr expr = x.getExpr();
      List<SQLExpr> targetList = x.getTargetList();
      return false;
    }

    @Override
    public void endVisit (SQLContainsExpr x){
      boolean not = x.isNot();
      SQLExpr expr = x.getExpr();
      List<SQLExpr> targetList = x.getTargetList();
    }

    @Override
    public boolean visit (SQLRealExpr x){
      Float value = x.getValue();
      return false;
    }

    @Override
    public void endVisit (SQLRealExpr x){
      Float value = x.getValue();
    }

    @Override
    public boolean visit (SQLWindow x){
      SQLName name = x.getName();
      SQLOver over = x.getOver();
      return false;
    }

    @Override
    public void endVisit (SQLWindow x){
      SQLName name = x.getName();
      SQLOver over = x.getOver();
    }

    @Override
    public boolean visit (SQLDumpStatement x){
      boolean overwrite = x.isOverwrite();
      SQLExprTableSource into = x.getInto();
      SQLSelect select = x.getSelect();
      return false;
    }

    @Override
    public void endVisit (SQLDumpStatement x){
      boolean overwrite = x.isOverwrite();
      SQLExprTableSource into = x.getInto();
      SQLSelect select = x.getSelect();
    }
  }
