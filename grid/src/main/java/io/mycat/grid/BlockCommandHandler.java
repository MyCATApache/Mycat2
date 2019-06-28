package io.mycat.grid;

import io.mycat.command.AbstractCommandHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class BlockCommandHandler extends AbstractCommandHandler {

  final static ExecutorService service = Executors
      .newCachedThreadPool(r -> new ReactorEnvThread(r) {
      });
  GridCommandHandler handler;

  public BlockCommandHandler(MycatRouter router, ProxyRuntime runtime) {
    handler = new GridCommandHandler();
  }

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {

  }

  @Override
  public void handleQuery(byte[] sql, MycatSession mycatSession) {
    block(mycatSession, (session) -> {
      handler.handleQuery(sql, session);
    });
  }

  public void block(MycatSession session, Consumer<MycatSession> consumer) {
    service.submit(() -> {
      ReactorEnvThread thread = null;
      try {
        thread = (ReactorEnvThread) Thread.currentThread();
        session.deliverWorkerThread(thread);
        consumer.accept(session);
      } catch (Exception e) {
        session.setLastMessage(e.toString());
        session.writeErrorEndPacket();
      } finally {
        session.backFromWorkerThread(thread);
      }
    });
  }

  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession session) {
    handler.handleContentOfFilename(sql, session);
  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession mycat) {
    block(mycat, (session -> {
      handler.handleContentOfFilenameEmptyOk(session);
    }));
  }

  @Override
  public void handleQuit(MycatSession session) {

  }

  @Override
  public void handleInitDb(String db, MycatSession session) {

  }

  @Override
  public void handlePing(MycatSession session) {

  }

  @Override
  public void handleFieldList(String table, String filedWildcard, MycatSession session) {

  }

  @Override
  public void handleSetOption(boolean on, MycatSession session) {

  }

  @Override
  public void handleCreateDb(String schemaName, MycatSession session) {

  }

  @Override
  public void handleDropDb(String schemaName, MycatSession session) {

  }

  @Override
  public void handleStatistics(MycatSession session) {

  }

  @Override
  public void handleProcessInfo(MycatSession session) {

  }

  @Override
  public void handleProcessKill(long connectionId, MycatSession session) {

  }

  @Override
  public void handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSession session) {

  }

  @Override
  public void handleResetConnection(MycatSession session) {

  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSession mycat) {
    block(mycat, (session -> {
      handler.handlePrepareStatement(sql, session);
    }));
  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession session) {
    handler.handlePrepareStatementLongdata(statementId, paramId, data, session);
  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams, byte[] rest, MycatSession mycat) {
    block(mycat, (session -> {
      handler.handlePrepareStatementExecute(rawPayload, statementId, flags, numParams, rest, mycat);
    }));
  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {
    handler.handlePrepareStatementClose(statementId, session);
  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row,
      MycatSession mycat) {
    block(mycat, (session -> {
      handler.handlePrepareStatementFetch(statementId, row, mycat);
    }));
  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession mycat) {
    block(mycat, (session -> {
      handler.handlePrepareStatementReset(statementId, mycat);
    }));
  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession mycat) {
    return handler.getNumParamsByStatementId(statementId, mycat);
  }
}