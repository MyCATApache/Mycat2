package io.mycat.command;

import io.mycat.MycatException;
import io.mycat.proxy.session.MycatSession;
import io.vertx.core.Future;

import java.util.Map;

public abstract class AbstractCommandHandler implements CommandDispatcher {


    public Future<Void> handleSleep(MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleSleep"));
    }


    public Future<Void> handleRefresh(int subCommand, MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleRefresh"));
    }


    public Future<Void> handleShutdown(int shutdownType, MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleShutdown"));
    }


    public Future<Void> handleConnect(MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleConnect"));
    }


    public Future<Void> handleDebug(MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleDebug"));
    }


    public Future<Void> handleTime(MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleTime"));
    }


    public Future<Void> handleDelayedInsert(MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleDelayedInsert"));
    }


    public Future<Void> handleDaemon(MycatSession session) {
        return Future.failedFuture(new MycatException("unsupport handleDaemon"));
    }

    @Override
    public Future<Void> handleInitDb(String db, MycatSession mycat) {
        mycat.useSchema(db);
        return mycat.writeOkEndPacket();
    }

    @Override
    public Future<Void> handleQuit(MycatSession mycat) {
        return mycat.close(true, "quit");
    }

    @Override
    public Future<Void> handlePing(MycatSession mycat) {
        return mycat.writeOkEndPacket();
    }

    @Override
    public Future<Void> handleFieldList(String table, String filedWildcard, MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleFieldList"));
    }

    @Override
    public Future<Void> handleSetOption(boolean on, MycatSession mycat) {
        mycat.setMultiStatementSupport(on);
        return mycat.writeOkEndPacket();
    }

    @Override
    public Future<Void> handleCreateDb(String schemaName, MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleCreateDb"));
    }

    @Override
    public Future<Void> handleDropDb(String schemaName, MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleCreateDb"));
    }

    @Override
    public Future<Void> handleStatistics(MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleStatistics"));
    }

    @Override
    public Future<Void> handleProcessInfo(MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleProcessInfo"));
    }

    @Override
    public Future<Void> handleChangeUser(String userName, String authResponse, String schemaName,
                                         int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                         MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleChangeUser"));
    }

    @Override
    public Future<Void> handleResetConnection(MycatSession mycat) {
        mycat.resetSession();
        return Future.failedFuture(new MycatException("unsupport  handleResetConnection"));
    }

    @Override
    public Future<Void> handleProcessKill(long connectionId, MycatSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleProcessKill"));
    }
}
