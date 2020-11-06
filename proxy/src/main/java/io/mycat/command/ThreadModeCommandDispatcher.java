package io.mycat.command;

import io.mycat.BindValue;
import io.mycat.proxy.session.MycatSession;

import java.util.Map;

/**
 * 该类主要解决事务管理与线程以及报文写入结束的关系
 * 装饰者模式
 */
public abstract class ThreadModeCommandDispatcher implements CommandDispatcher {
    final CommandDispatcher dispatcher;

    public ThreadModeCommandDispatcher(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void initRuntime(MycatSession session) {
        dispatcher.initRuntime(session);
    }

    @Override
    public void handleQuery(byte[] sql, MycatSession session) {
        run(session, () -> dispatcher.handleQuery(sql, session));
    }

    @Override
    public void handleContentOfFilename(byte[] sql, MycatSession session) {
        run(session, () -> dispatcher.handleContentOfFilename(sql, session));
    }

    @Override
    public void handleContentOfFilenameEmptyOk(MycatSession session) {
        run(session, () -> dispatcher.handleContentOfFilenameEmptyOk(session));
    }

    @Override
    public void handleSleep(MycatSession session) {
        run(session, () -> dispatcher.handleSleep(session));
    }

    @Override
    public void handleQuit(MycatSession session) {
        run(session, () -> dispatcher.handleQuit(session));
    }

    @Override
    public void handleInitDb(String db, MycatSession session) {
        run(session, () -> dispatcher.handleInitDb(db, session));
    }

    @Override
    public void handlePing(MycatSession session) {
        run(session, () -> dispatcher.handlePing(session));
    }

    @Override
    public void handleFieldList(String table, String filedWildcard, MycatSession session) {
        run(session, () -> dispatcher.handleFieldList(table, filedWildcard, session));
    }

    @Override
    public void handleSetOption(boolean on, MycatSession session) {
        run(session, () -> dispatcher.handleSetOption(on, session));
    }

    @Override
    public void handleCreateDb(String schemaName, MycatSession session) {
        run(session, () -> dispatcher.handleCreateDb(schemaName, session));
    }

    @Override
    public void handleDropDb(String schemaName, MycatSession session) {
        run(session, () -> dispatcher.handleDropDb(schemaName, session));
    }

    @Override
    public void handleRefresh(int subCommand, MycatSession session) {
        run(session, () -> dispatcher.handleRefresh(subCommand, session));
    }

    @Override
    public void handleShutdown(int shutdownType, MycatSession session) {
        run(session, () -> dispatcher.handleShutdown(shutdownType, session));
    }

    @Override
    public void handleStatistics(MycatSession session) {
        run(session, () -> dispatcher.handleStatistics(session));
    }

    @Override
    public void handleProcessInfo(MycatSession session) {
        run(session, () -> dispatcher.handleProcessInfo(session));
    }

    @Override
    public void handleConnect(MycatSession session) {
        run(session, () -> dispatcher.handleConnect(session));
    }

    @Override
    public void handleProcessKill(long connectionId, MycatSession session) {
        run(session, () -> dispatcher.handleProcessKill(connectionId, session));
    }

    @Override
    public void handleDebug(MycatSession session) {
        run(session, () -> dispatcher.handleDebug(session));
    }

    @Override
    public void handleTime(MycatSession session) {
        run(session, () -> dispatcher.handleTime(session));
    }

    @Override
    public void handleChangeUser(String userName, String authResponse, String schemaName, int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs, MycatSession session) {
        run(session, () -> dispatcher.handleChangeUser(userName, authResponse, schemaName, charsetSet, authPlugin, clientConnectAttrs, session));
    }

    @Override
    public void handleDelayedInsert(MycatSession session) {
        run(session, () -> dispatcher.handleDelayedInsert(session));
    }

    @Override
    public void handleResetConnection(MycatSession session) {
        run(session, () -> dispatcher.handleResetConnection(session));
    }

    @Override
    public void handleDaemon(MycatSession session) {
        run(session, () -> dispatcher.handleDaemon(session));
    }

    @Override
    public void handlePrepareStatement(byte[] sql, MycatSession session) {
        run(session, () -> dispatcher.handlePrepareStatement(sql, session));
    }

    @Override
    public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatSession session) {
        run(session, () -> dispatcher.handlePrepareStatementLongdata(statementId, paramId, data, session));
    }

    @Override
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int[] params, BindValue[] values, MycatSession session) {
        run(session, () -> dispatcher.handlePrepareStatementExecute(rawPayload, statementId, flags, params, values, session));
    }

    @Override
    public void handlePrepareStatementClose(long statementId, MycatSession session) {
        run(session, () -> dispatcher.handlePrepareStatementClose(statementId, session));
    }

    @Override
    public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {
        run(session, () -> dispatcher.handlePrepareStatementFetch(statementId, row, session));
    }

    @Override
    public void handlePrepareStatementReset(long statementId, MycatSession session) {
        run(session, () -> dispatcher.handlePrepareStatementReset(statementId, session));
    }

    @Override
    public int getNumParamsByStatementId(long statementId, MycatSession session) {
        return dispatcher.getNumParamsByStatementId(statementId, session);
    }

    protected abstract void run(MycatSession session, Runnable runnable);

    @Override
    public byte[] getLongData(long statementId, int i, MycatSession mycat) {
        return dispatcher.getLongData(statementId, i,mycat);
    }

    @Override
    public BindValue[] getLastBindValue(long statementId, MycatSession mycat) {
        return dispatcher.getLastBindValue(statementId, mycat);
    }

    @Override
    public void saveBindValue(long statementId, BindValue[] values, MycatSession mycat) {
        dispatcher.saveBindValue(statementId, values, mycat);
    }
}