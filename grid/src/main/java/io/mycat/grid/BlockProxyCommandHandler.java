package io.mycat.grid;

import io.mycat.bindThread.BindThreadKey;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.thread.GProcess;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.MycatSession;
import java.util.function.Consumer;

public class BlockProxyCommandHandler extends AbstractCommandHandler {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(BlockProxyCommandHandler.class);
  final GridProxyCommandHandler handler;
  private GRuntime jdbcRuntime;

  public BlockProxyCommandHandler() {
    handler = new GridProxyCommandHandler();
  }

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {
    handler.initRuntime(session, runtime);
    this.jdbcRuntime = (GRuntime) runtime.getDefContext().get("gridRuntime");
  }

  @Override
  public void handleQuery(byte[] sql, MycatSession mycatSession) {
    block(mycatSession, (session) -> {
      String s = new String(sql);
      System.out.println(s);
      handler.handleQuery(sql, session);
    });
  }

  public void block(MycatSession mycat, Consumer<MycatSession> consumer) {
    GRuntime.INSTACNE.run(mycat, new GProcess() {
      @Override
      public void accept(BindThreadKey key, TransactionSession session) {
        try {
          mycat.deliverWorkerThread((SessionThread) Thread.currentThread());
          consumer.accept(mycat);
        } finally {
          mycat.backFromWorkerThread();
        }
      }

      @Override
      public void onException(BindThreadKey key, Exception e) {
        LOGGER.error("", e);
        mycat.setLastMessage(e.toString());
        mycat.writeErrorEndPacket();
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