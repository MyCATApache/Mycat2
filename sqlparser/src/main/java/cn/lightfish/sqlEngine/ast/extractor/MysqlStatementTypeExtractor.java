/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.sqlEngine.ast.extractor;

import cn.lightfish.sqlEngine.schema.StatementType;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.clause.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import static cn.lightfish.sqlEngine.schema.StatementType.UNKNOW;

/**
 * chenjunwen 294712221@qq.com
 */
public class MysqlStatementTypeExtractor extends MySqlASTVisitorAdapter {
    StatementType statementType = UNKNOW;

    public StatementType getStatementType() {
        return statementType;
    }

    public MysqlStatementTypeExtractor() {
        super();
    }
    @Override
    public boolean visit(SQLSetStatement x) {
        statementType = StatementType.SQLSetStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlPrepareStatement x) {
        statementType = StatementType.MySqlPrepareStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlExecuteStatement x) {
        statementType = StatementType.MySqlExecuteStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlDeallocatePrepareStatement x) {
        statementType = StatementType.MysqlDeallocatePrepareStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        statementType = StatementType.MySqlDeleteStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlInsertStatement x) {
        statementType = StatementType.MySqlInsertStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlLoadDataInFileStatement x) {
        statementType = StatementType.MySqlInsertStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlLoadXmlStatement x) {
        statementType = StatementType.MySqlLoadXmlStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowColumnsStatement x) {
        statementType = StatementType.SQLShowColumnsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowDatabaseStatusStatement x) {
        statementType = StatementType.MySqlShowDatabaseStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowWarningsStatement x) {
        statementType = StatementType.MySqlShowWarningsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowStatusStatement x) {
        statementType = StatementType.MySqlShowStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlKillStatement x) {
        statementType = StatementType.MySqlKillStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlBinlogStatement x) {
        statementType = StatementType.MySqlBinlogStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlResetStatement x) {
        statementType = StatementType.MySqlResetStatement;
        return false;
    }


    @Override
    public boolean visit(MySqlCreateUserStatement x) {
        statementType = StatementType.MySqlCreateUserStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlUpdatePlanCacheStatement x) {
        statementType = StatementType.MySqlUpdatePlanCacheStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowPlanCacheStatusStatement x) {
        statementType = StatementType.MySqlShowPlanCacheStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlClearPlanCacheStatement x) {
        statementType = StatementType.MySqlClearPlanCacheStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlDisabledPlanCacheStatement x) {
        statementType = StatementType.MySqlDisabledPlanCacheStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlExplainPlanCacheStatement x) {
        statementType = StatementType.MySqlExplainPlanCacheStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlExplainStatement x) {
        statementType = StatementType.MySqlExplainStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {
        statementType = StatementType.MySqlUpdateStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlSetTransactionStatement x) {
        statementType = StatementType.MySqlSetTransactionStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowHMSMetaStatement x) {
        statementType = StatementType.MySqlShowHMSMetaStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowAuthorsStatement x) {
        statementType = StatementType.MySqlShowAuthorsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowBinaryLogsStatement x) {
        statementType = StatementType.MySqlShowBinaryLogsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowMasterLogsStatement x) {
        statementType = StatementType.MySqlShowMasterLogsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCollationStatement x) {
        statementType = StatementType.MySqlShowCollationStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowBinLogEventsStatement x) {
        statementType = StatementType.MySqlShowBinLogEventsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCharacterSetStatement x) {
        statementType = StatementType.MySqlShowCharacterSetStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowContributorsStatement x) {
        statementType = StatementType.MySqlShowContributorsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowTopologyStatement x) {
        statementType = StatementType.MySqlShowTopologyStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateDatabaseStatement x) {
        statementType = StatementType.MySqlShowCreateDatabaseStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateEventStatement x) {
        statementType = StatementType.MySqlShowCreateEventStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateFunctionStatement x) {
        statementType = StatementType.MySqlShowCreateFunctionStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateProcedureStatement x) {
        statementType = StatementType.MySqlShowCreateProcedureStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowCreateTableStatement x) {
        statementType = StatementType.SQLShowCreateTableStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowCreateTriggerStatement x) {
        statementType = StatementType.MySqlShowCreateTriggerStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowEngineStatement x) {
        statementType = StatementType.MySqlShowEngineStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowEnginesStatement x) {
        statementType = StatementType.MySqlShowEnginesStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowErrorsStatement x) {
        statementType = StatementType.MySqlShowErrorsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowEventsStatement x) {
        statementType = StatementType.MySqlShowEventsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowFunctionCodeStatement x) {
        statementType = StatementType.MySqlShowFunctionCodeStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowFunctionStatusStatement x) {
        statementType = StatementType.MySqlShowFunctionStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowGrantsStatement x) {
        statementType = StatementType.MySqlShowGrantsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowMasterStatusStatement x) {
        statementType = StatementType.MySqlShowMasterStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowOpenTablesStatement x) {
        statementType = StatementType.MySqlShowOpenTablesStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowPluginsStatement x) {
        statementType = StatementType.MySqlShowPluginsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowPartitionsStatement x) {
        statementType = StatementType.MySqlShowPartitionsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowPrivilegesStatement x) {
        statementType = StatementType.MySqlShowPrivilegesStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowProcedureCodeStatement x) {
        statementType = StatementType.MySqlShowProcedureCodeStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowProcedureStatusStatement x) {
        statementType = StatementType.MySqlShowProcedureStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowProcessListStatement x) {
        statementType = StatementType.MySqlShowProcessListStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowProfileStatement x) {
        statementType = StatementType.MySqlShowProfileStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowProfilesStatement x) {
        statementType = StatementType.MySqlShowProfileStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowRelayLogEventsStatement x) {
        statementType = StatementType.MySqlShowRelayLogEventsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowRuleStatement x) {
        statementType = StatementType.MySqlShowRuleStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowRuleStatusStatement x) {
        statementType = StatementType.MySqlShowRuleStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowSlaveHostsStatement x) {
        statementType = StatementType.MySqlShowSlaveHostsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowSequencesStatement x) {
        statementType = StatementType.MySqlShowSequencesStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCreateExternalCatalogStatement x) {
        statementType = StatementType.MySqlCreateExternalCatalogStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowSlowStatement x) {
        statementType = StatementType.MySqlShowSlowStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowSlaveStatusStatement x) {
        statementType = StatementType.MySqlShowSlaveStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowTableStatusStatement x) {
        statementType = StatementType.MySqlShowTableStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlShowDbLockStatement x) {
        statementType = StatementType.MysqlShowDbLockStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlShowHtcStatement x) {
        statementType = StatementType.MysqlShowHtcStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlShowStcStatement x) {
        statementType = StatementType.MysqlShowStcStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowTriggersStatement x) {
        statementType = StatementType.MysqlShowStcStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowTraceStatement x) {
        statementType = StatementType.MySqlShowTraceStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowBroadcastsStatement x) {
        statementType = StatementType.MySqlShowBroadcastsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowDdlStatusStatement x) {
        statementType = StatementType.MySqlShowDdlStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowDsStatement x) {
        statementType = StatementType.MySqlShowDsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowVariantsStatement x) {
        statementType = StatementType.MySqlShowVariantsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlRenameTableStatement x) {
        statementType = StatementType.MySqlRenameTableStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlLockTableStatement x) {
        statementType = StatementType.MySqlLockTableStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlUnlockTablesStatement x) {
        statementType = StatementType.MySqlUnlockTablesStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCreateTableStatement x) {
        statementType = StatementType.MySqlCreateTableStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlHelpStatement x) {
        statementType = StatementType.MySqlHelpStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlAnalyzeStatement x) {
        statementType = StatementType.MySqlAnalyzeStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlAlterUserStatement x) {
        statementType = StatementType.MySqlAlterUserStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlOptimizeStatement x) {
        statementType = StatementType.MySqlOptimizeStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlHintStatement x) {
        statementType = StatementType.MySqlHintStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCaseStatement x) {
        statementType = StatementType.MySqlCaseStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlDeclareStatement x) {
        statementType = StatementType.MySqlDeclareStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlSelectIntoStatement x) {
        statementType = StatementType.MySqlSelectIntoStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCaseStatement.MySqlWhenStatement x) {
        statementType = StatementType.MySqlWhenStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlLeaveStatement x) {
        statementType = StatementType.MySqlLeaveStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlIterateStatement x) {
        statementType = StatementType.MySqlIterateStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlRepeatStatement x) {
        statementType = StatementType.MySqlRepeatStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCursorDeclareStatement x) {
        statementType = StatementType.MySqlCursorDeclareStatement;
        return false;
    }


    @Override
    public boolean visit(MySqlDeclareHandlerStatement x) {
        statementType = StatementType.MySqlDeclareHandlerStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlDeclareConditionStatement x) {
        statementType = StatementType.MySqlDeclareConditionStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlFlushStatement x) {
        statementType = StatementType.MySqlFlushStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlEventSchedule x) {
        statementType = StatementType.MySqlEventSchedule;
        return false;
    }

    @Override
    public boolean visit(MySqlCreateEventStatement x) {
        statementType = StatementType.MySqlCreateEventStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCreateAddLogFileGroupStatement x) {
        statementType = StatementType.MySqlCreateAddLogFileGroupStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCreateServerStatement x) {
        statementType = StatementType.MySqlCreateServerStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlCreateTableSpaceStatement x) {
        statementType = StatementType.MySqlCreateTableSpaceStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlAlterEventStatement x) {
        statementType = StatementType.MySqlAlterEventStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlAlterLogFileGroupStatement x) {
        statementType = StatementType.MySqlAlterLogFileGroupStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlAlterServerStatement x) {
        statementType = StatementType.MySqlAlterServerStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlAlterTablespaceStatement x) {
        statementType = StatementType.MySqlAlterTablespaceStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlChecksumTableStatement x) {
        statementType = StatementType.MySqlChecksumTableStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowDatasourcesStatement x) {
        statementType = StatementType.MySqlShowDatasourcesStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowNodeStatement x) {
        statementType = StatementType.MySqlShowNodeStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowHelpStatement x) {
        statementType = StatementType.MySqlShowHelpStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlFlashbackStatement x) {
        statementType = StatementType.MySqlFlashbackStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowConfigStatement x) {
        statementType = StatementType.MySqlShowConfigStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowPlanCacheStatement x) {
        statementType = StatementType.MySqlShowPlanCacheStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowPhysicalProcesslistStatement x) {
        statementType = StatementType.MySqlShowPhysicalProcesslistStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlRenameSequenceStatement x) {
        statementType = StatementType.MySqlRenameSequenceStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlCreateFullTextCharFilterStatement x) {
        statementType = StatementType.MysqlCreateFullTextCharFilterStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlShowFullTextStatement x) {
        statementType = StatementType.MysqlShowFullTextStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlShowCreateFullTextStatement x) {
        statementType = StatementType.MysqlShowCreateFullTextStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlAlterFullTextStatement x) {
        statementType = StatementType.MysqlAlterFullTextStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlDropFullTextStatement x) {
        statementType = StatementType.MysqlDropFullTextStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlCreateFullTextTokenizerStatement x) {
        statementType = StatementType.MysqlCreateFullTextTokenizerStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlCreateFullTextTokenFilterStatement x) {
        statementType = StatementType.MysqlCreateFullTextTokenFilterStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlCreateFullTextAnalyzerStatement x) {
        statementType = StatementType.MysqlCreateFullTextAnalyzerStatement;
        return false;
    }

    @Override
    public boolean visit(MysqlCreateFullTextDictionaryStatement x) {
        statementType = StatementType.MysqlCreateFullTextDictionaryStatement;
        return false;
    }


    @Override
    public boolean visit(MySqlExecuteForAdsStatement x) {
        statementType = StatementType.MySqlExecuteForAdsStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlManageInstanceGroupStatement x) {
        statementType = StatementType.MySqlManageInstanceGroupStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlRaftMemberChangeStatement x) {
        statementType = StatementType.MySqlRaftMemberChangeStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlRaftLeaderTransferStatement x) {
        statementType = StatementType.MySqlRaftLeaderTransferStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlMigrateStatement x) {
        statementType = StatementType.MySqlMigrateStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowClusterNameStatement x) {
        statementType = StatementType.MySqlShowJobStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowJobStatusStatement x) {
        statementType = StatementType.MySqlShowJobStatusStatement;
        return false;
    }

    @Override
    public boolean visit(MySqlShowMigrateTaskStatusStatement x) {
        statementType = StatementType.MySqlShowMigrateTaskStatusStatement;
        return false;
    }


    @Override
    public boolean visit(MySqlCheckTableStatement x) {
        statementType = StatementType.MySqlCheckTableStatement;
        return false;
    }
    
    @Override
    public boolean visit(SQLSelectStatement x) {
        statementType = StatementType.MySqlCheckTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropTableStatement x) {
        statementType = StatementType.SQLDropTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateTableStatement x) {
        statementType = StatementType.SQLCreateTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDeleteStatement x) {
        statementType = StatementType.SQLDeleteStatement;
        return false;
    }

    @Override
    public boolean visit(SQLInsertStatement x) {
        statementType = StatementType.SQLInsertStatement;
        return false;
    }

    @Override
    public boolean visit(SQLUpdateStatement x) {
        statementType = StatementType.SQLUpdateStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateViewStatement x) {
        statementType = StatementType.SQLCreateViewStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterViewStatement x) {
        statementType = StatementType.SQLAlterViewStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableGroupStatement x) {
        statementType = StatementType.SQLAlterTableGroupStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterSystemGetConfigStatement x) {
        statementType = StatementType.SQLAlterSystemGetConfigStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterSystemSetConfigStatement x) {
        statementType = StatementType.SQLAlterSystemSetConfigStatement;
        return false;
    }

    @Override
    public boolean visit(SQLTruncateStatement x) {
        statementType = StatementType.SQLTruncateStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCommentStatement x) {
        statementType = StatementType.SQLCommentStatement;
        return false;
    }

    @Override
    public boolean visit(SQLUseStatement x) {
        statementType = StatementType.SQLUseStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropIndexStatement x) {
        statementType = StatementType.SQLDropIndexStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropViewStatement x) {
        statementType = StatementType.SQLDropViewStatement;
        return false;
    }

    @Override
    public boolean visit(SQLSavePointStatement x) {
        statementType = StatementType.SQLSavePointStatement;
        return false;
    }

    @Override
    public boolean visit(SQLRollbackStatement x) {
        statementType = StatementType.SQLRollbackStatement;
        return false;
    }

    @Override
    public boolean visit(SQLReleaseSavePointStatement x) {
        statementType = StatementType.SQLReleaseSavePointStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateDatabaseStatement x) {
        statementType = StatementType.SQLCreateDatabaseStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateIndexStatement x) {
        statementType = StatementType.SQLCreateIndexStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropSequenceStatement x) {
        statementType = StatementType.SQLDropSequenceStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropTriggerStatement x) {
        statementType = StatementType.SQLDropTriggerStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropUserStatement x) {
        statementType = StatementType.SQLDropUserStatement;
        return false;
    }

    @Override
    public boolean visit(SQLExplainStatement x) {
        statementType = StatementType.SQLExplainStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropDatabaseStatement x) {
        statementType = StatementType.SQLDropDatabaseStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateTriggerStatement x) {
        statementType = StatementType.SQLCreateTriggerStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropFunctionStatement x) {
        statementType = StatementType.SQLDropFunctionStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropTableSpaceStatement x) {
        statementType = StatementType.SQLDropTableSpaceStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropProcedureStatement x) {
        statementType = StatementType.SQLDropProcedureStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropCatalogStatement x) {
        statementType = StatementType.SQLDropCatalogStatement;
        return false;
    }

    @Override
    public boolean visit(SQLRevokeStatement x) {
        statementType = StatementType.SQLRevokeStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterViewRenameStatement x) {
        statementType = StatementType.SQLAlterViewRenameStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowTablesStatement x) {
        statementType = StatementType.SQLShowTablesStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateProcedureStatement x) {
        statementType = StatementType.SQLCreateProcedureStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateFunctionStatement x) {
        statementType = StatementType.SQLCreateFunctionStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterDatabaseStatement x) {
        statementType = StatementType.SQLAlterDatabaseStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateSequenceStatement x) {
        statementType = StatementType.SQLCreateSequenceStatement;
        return false;
    }

    @Override
    public boolean visit(SQLStartTransactionStatement x) {
        statementType = StatementType.SQLStartTransactionStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDescribeStatement x) {
        statementType = StatementType.SQLDescribeStatement;
        return false;
    }

    @Override
    public boolean visit(SQLWhileStatement x) {
        statementType = StatementType.SQLWhileStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDeclareStatement x) {
        statementType = StatementType.SQLDeclareStatement;
        return false;
    }

    @Override
    public boolean visit(SQLReturnStatement x) {
        statementType = StatementType.SQLReturnStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCommitStatement x) {
        statementType = StatementType.SQLCommitStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateMaterializedViewStatement x) {
        statementType = StatementType.SQLCreateMaterializedViewStatement;
        return false;
    }

    @Override
    public boolean visit(SQLScriptCommitStatement x) {
        statementType = StatementType.SQLScriptCommitStatement;
        return false;
    }

    @Override
    public boolean visit(SQLReplaceStatement x) {
        statementType = StatementType.SQLReplaceStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateUserStatement x) {
        statementType = StatementType.SQLCreateUserStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterFunctionStatement x) {
        statementType = StatementType.SQLAlterFunctionStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowErrorsStatement x) {
        statementType = StatementType.SQLShowErrorsStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowRecylebinStatement x) {
        statementType = StatementType.SQLShowRecylebinStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterProcedureStatement x) {
        statementType = StatementType.SQLAlterProcedureStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropEventStatement x) {
        statementType = StatementType.SQLDropEventStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropLogFileGroupStatement x) {
        statementType = StatementType.SQLDropLogFileGroupStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropServerStatement x) {
        statementType = StatementType.SQLDropServerStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropSynonymStatement x) {
        statementType = StatementType.SQLDropSynonymStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropTypeStatement x) {
        statementType = StatementType.SQLDropTypeStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropMaterializedViewStatement x) {
        statementType = StatementType.SQLDropMaterializedViewStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateTableGroupStatement x) {
        statementType = StatementType.SQLCreateTableGroupStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropTableGroupStatement x) {
        statementType = StatementType.SQLDropTableGroupStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowDatabasesStatement x) {
        statementType = StatementType.SQLShowDatabasesStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowTableGroupsStatement x) {
        statementType = StatementType.SQLShowTableGroupsStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowProcessListStatement x) {
        statementType = StatementType.SQLShowProcessListStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowCreateViewStatement x) {
        statementType = StatementType.SQLShowCreateViewStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowViewsStatement x) {
        statementType = StatementType.SQLShowViewsStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterSequenceStatement x) {
        statementType = StatementType.SQLAlterSequenceStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateRoleStatement x) {
        statementType = StatementType.SQLCreateRoleStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropRoleStatement x) {
        statementType = StatementType.SQLDropRoleStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowPartitionsStmt x) {
        statementType = StatementType.SQLShowPartitionsStmt;
        return false;
    }

    @Override
    public boolean visit(SQLDumpStatement x) {
        statementType = StatementType.SQLDumpStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowIndexesStatement x) {
        statementType = StatementType.SQLShowIndexesStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAnalyzeTableStatement x) {
        statementType = StatementType.SQLAnalyzeTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLExportTableStatement x) {
        statementType = StatementType.SQLExportTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLImportTableStatement x) {
        statementType = StatementType.SQLImportTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterIndexStatement x) {
        statementType = StatementType.SQLAlterIndexStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCreateOutlineStatement x) {
        statementType = StatementType.SQLCreateOutlineStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropOutlineStatement x) {
        statementType = StatementType.SQLDropOutlineStatement;
        return false;
    }

    @Override
    public boolean visit(SQLAlterOutlineStatement x) {
        statementType = StatementType.SQLAlterOutlineStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowOutlinesStatement x) {
        statementType = StatementType.SQLShowOutlinesStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowQueryTaskStatement x) {
        statementType = StatementType.SQLShowQueryTaskStatement;
        return false;
    }

    @Override
    public boolean visit(SQLPurgeTableStatement x) {
        statementType = StatementType.SQLPurgeTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLPurgeLogsStatement x) {
        statementType = StatementType.SQLPurgeLogsStatement;
        return false;
    }

    @Override
    public boolean visit(SQLPurgeRecyclebinStatement x) {
        statementType = StatementType.SQLPurgeRecyclebinStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowStatisticStmt x) {
        statementType = StatementType.SQLShowStatisticStmt;
        return false;
    }

    @Override
    public boolean visit(SQLShowCatalogsStatement x) {
        statementType = StatementType.SQLShowCatalogsStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowFunctionsStatement x) {
        statementType = StatementType.SQLShowFunctionsStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowSessionStatement x) {
        statementType = StatementType.SQLShowSessionStatement;
        return false;
    }

    @Override
    public boolean visit(SQLExplainAnalyzeStatement x) {
        statementType = StatementType.SQLExplainAnalyzeStatement;
        return false;
    }

    @Override
    public boolean visit(SQLWhoamiStatement x) {
        statementType = StatementType.SQLWhoamiStatement;
        return false;
    }

    @Override
    public boolean visit(SQLDropResourceStatement x) {
        statementType = StatementType.SQLDropResourceStatement;
        return false;
    }

    @Override
    public boolean visit(SQLCopyFromStatement x) {
        statementType = StatementType.SQLCopyFromStatement;
        return false;
    }

    @Override
    public boolean visit(SQLShowUsersStatement x) {
        statementType = StatementType.SQLShowUsersStatement;
        return false;
    }

    @Override
    public boolean visit(SQLSubmitJobStatement x) {
        statementType = StatementType.SQLSubmitJobStatement;
        return false;
    }

    @Override
    public boolean visit(SQLSyncMetaStatement x) {
        statementType = StatementType.SQLSyncMetaStatement;
        return false;
    }

    @Override
    public boolean visit(SQLArchiveTableStatement x) {
        statementType = StatementType.SQLArchiveTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLBackupStatement x) {
        statementType = StatementType.SQLBackupStatement;
        return false;
    }

    @Override
    public boolean visit(SQLRestoreStatement x) {
        statementType = StatementType.SQLRestoreStatement;
        return false;
    }

    @Override
    public boolean visit(SQLBuildTableStatement x) {
        statementType = StatementType.SQLBuildTableStatement;
        return false;
    }

    @Override
    public boolean visit(SQLExportDatabaseStatement x) {
        statementType = StatementType.SQLExportDatabaseStatement;
        return false;
    }

    @Override
    public boolean visit(SQLImportDatabaseStatement x) {
        statementType = StatementType.SQLImportDatabaseStatement;
        return false;
    }

    @Override
    public boolean visit(SQLRenameUserStatement x) {
        statementType = StatementType.SQLRenameUserStatement;
        return false;
    }
}