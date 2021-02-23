package io.mycat.command;

import io.mycat.MycatException;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.proxy.session.MycatSession;
import io.vertx.core.Future;

import java.util.Map;

public abstract class AbstractCommandHandler implements CommandDispatcher {


    public Future<Void> handleSleep(MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleSleep"));
    }


    public Future<Void> handleRefresh(int subCommand, MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleRefresh"));
    }


    public Future<Void> handleShutdown(int shutdownType, MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleShutdown"));
    }


    public Future<Void> handleConnect(MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleConnect"));
    }


    public Future<Void> handleDebug(MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleDebug"));
    }


    public Future<Void> handleTime(MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleTime"));
    }


    public Future<Void> handleDelayedInsert(MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleDelayedInsert"));
    }


    public Future<Void> handleDaemon(MySQLServerSession session) {
        return Future.failedFuture(new MycatException("unsupport handleDaemon"));
    }

    @Override
    public Future<Void> handleInitDb(String db, MySQLServerSession mycat) {
        mycat.getDataContext().useShcema(db);
        return mycat.writeOkEndPacket();
    }

    @Override
    public Future<Void> handleQuit(MySQLServerSession mycat) {
        return mycat.close(true, "quit");
    }

    @Override
    public Future<Void> handlePing(MySQLServerSession mycat) {
        return mycat.writeOkEndPacket();
    }

    @Override
    public Future<Void> handleFieldList(String table, String filedWildcard, MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleFieldList"));
    }

    @Override
    public Future<Void> handleSetOption(boolean on, MySQLServerSession mycat) {
        return mycat.writeOkEndPacket();
    }

    @Override
    public Future<Void> handleCreateDb(String schemaName, MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleCreateDb"));
    }

    @Override
    public Future<Void> handleDropDb(String schemaName, MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleCreateDb"));
    }

    @Override
    public Future<Void> handleStatistics(MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleStatistics"));
    }

    @Override
    public Future<Void> handleProcessInfo(MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleProcessInfo"));
    }

    @Override
    public Future<Void> handleChangeUser(String userName, String authResponse, String schemaName,
                                         int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                         MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleChangeUser"));
    }

    @Override
    public Future<Void> handleResetConnection(MySQLServerSession mycat) {
        mycat.resetSession();
        return Future.failedFuture(new MycatException("unsupport  handleResetConnection"));
    }

    @Override
    public Future<Void> handleProcessKill(long connectionId, MySQLServerSession mycat) {
        return Future.failedFuture(new MycatException("unsupport  handleProcessKill"));
    }
}
