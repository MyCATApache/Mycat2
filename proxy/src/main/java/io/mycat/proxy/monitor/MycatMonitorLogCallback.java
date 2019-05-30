package io.mycat.proxy.monitor;

import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import io.mycat.util.DumpUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221 date 2019-05-20 11:52
 **/
public class MycatMonitorLogCallback implements MycatMonitorCallback {

  protected final static Logger LOGGER = LoggerFactory.getLogger(MycatMonitor.class);
  final static boolean record = true;
  final static boolean recordDump = false;
  final static boolean onSQL = false;

  @Override
  public void onMySQLSessionServerStatusChanged(Session session, int serverStatus) {
    if (record) {

      boolean hasFatch = MySQLPacketResolver.hasFatch(serverStatus);
      boolean hasMoreResult = MySQLPacketResolver.hasMoreResult(serverStatus);
      boolean hasTranscation = MySQLPacketResolver.hasTrans(serverStatus);
      LOGGER.info("session id:{}  serverStatus:{} hasFatch:{} hasMoreResult:{} hasTranscation:{}",
          session.sessionId(), serverStatus, hasFatch, hasMoreResult, hasTranscation);
    }
  }
  @Override
  public void onOrginSQL(Session session, String sql) {
    if (onSQL) {
      LOGGER.info("session id:{}  orginSQL:{} ", session.sessionId(), sql);
    }
  }

  @Override
  public void onRoute(Session session, String dataNode, byte[] payload) {
    if (onSQL) {
      LOGGER.info("session id:{} dataNode:{}  payload:{} ", session.sessionId(), dataNode, new String(payload));
    }
  }
static SeekableByteChannel bufferedWriter;
  static {
    try {
       bufferedWriter = Files.newByteChannel(new File("d:/sql.txt").toPath(), StandardOpenOption.WRITE);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public final void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
//    if (recordDump) {
//      DumpUtil.printAsHex(view, startIndex, len);
//    }
    ByteBuffer duplicate = view.duplicate();
    duplicate.position(startIndex);
    duplicate.limit(startIndex+len);
    try {
      bufferedWriter.write(duplicate);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onPayloadType(Session session, MySQLPayloadType type) {
    LOGGER.info("session id:{} payload:{} ", session.sessionId(),type);
  }

  @Override
  public final void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }
  @Override
  public final void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }
  @Override
  public final void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }
  @Override
  public final void onAllocateByteBuffer(ByteBuffer buffer) {
    if (record) {
      //    Thread.dumpStack();
      LOGGER.debug("{}  {}", MycatMonitorCallback.getSession(), buffer);
    }
  }
  @Override
  public final void onSynchronizationState(MySQLClientSession session) {
    MySQLAutoCommit automCommit = session.isAutomCommit();
    String characterSetResult = session.getCharacterSetResult();
    String charset = session.getCharset();
    MySQLIsolation isolation = session.getIsolation();
    MycatDataNode dataNode = session.getDataNode();
    if (record) {
      //    Thread.dumpStack();
      LOGGER.debug(
          "sessionId:{} dataNode:{} isolation: {} charset:{} automCommit:{} characterSetResult:{}",
          session.sessionId(), dataNode,
          isolation, charset, automCommit, characterSetResult);
    }
  }
  @Override
  public final void onRecycleByteBuffer(ByteBuffer buffer) {
    if (record) {
      LOGGER.debug("{}  {}", MycatMonitorCallback.getSession(), buffer);
    }
  }
  @Override
  public final void onExpandByteBuffer(ByteBuffer buffer) {
    if (record) {
      LOGGER.debug("{}  {}", MycatMonitorCallback.getSession(), buffer);
    }
  }
  @Override
  public final void onNewMycatSession(MycatSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }
  @Override
  public final void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{} {}", mycat, session);
    }
  }
  @Override
  public final void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{} {}", mycat, session);
    }
  }
  @Override
  public final void onCloseMycatSession(MycatSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }
  @Override
  public final void onNewMySQLSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }
  @Override
  public final void onAddIdleMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }
  @Override
  public final void onGetIdleMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }
  @Override
  public final void onCloseMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

}
