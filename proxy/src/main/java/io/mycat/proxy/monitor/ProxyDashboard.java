//package io.mycat.proxy.monitor;
//
//import io.mycat.beans.MySQLSessionMonopolizeType;
//import io.mycat.beans.mysql.MySQLAutoCommit;
//import io.mycat.beans.mysql.MySQLIsolation;
//import io.mycat.beans.mysql.packet.ProxyBuffer;
//import io.mycat.buffer.BufferPool;
//import io.mycat.logTip.MycatLogger;
//import io.mycat.logTip.MycatLoggerFactory;
//import io.mycat.proxy.ProxyRuntime;
//import io.mycat.proxy.reactor.MycatReactorThread;
//import io.mycat.proxy.session.MySQLClientSession;
//import io.mycat.proxy.session.MySQLSessionManager;
//import io.mycat.proxy.session.MycatSession;
//import io.mycat.proxy.session.MycatUser;
//import io.mycat.proxy.session.SessionManager.FrontSessionManager;
//import io.mycat.replica.MySQLDatasource;
//import io.mycat.util.JavaUtils;
//
//import java.nio.ByteBuffer;
//import java.nio.charset.Charset;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Queue;
//
//public enum ProxyDashboard {
//  INSTANCE;
//  protected final static MycatLogger LOGGER = MycatLoggerFactory.getLogger("resourceLogger");
//
//  public void collectInfo(ProxyRuntime runtime) {
//    LOGGER.info("---------------------------dashboard---------------------------");
//    Runtime rt = Runtime.getRuntime();
//    LOGGER.info("---------------------------env---------------------------");
//    long used = rt.totalMemory() - rt.freeMemory();
//    LOGGER.info("heap memory used:{}", JavaUtils.bytesToString(used));
//    LOGGER.info("---------------------------env---------------------------");
//    for (MycatReactorThread thread : runtime.getMycatReactorThreads()) {
//      BufferPool bufPool = thread.getBufPool();
//      LOGGER.info("threadId:{} io off heap buffer capacity:{}", thread.getId(),
//          JavaUtils.bytesToString(bufPool.capacity()));
//      FrontSessionManager<MycatSession> frontManager = thread.getFrontManager();
//      for (MycatSession mycat : frontManager.getAllSessions()) {
//        MycatUser user = mycat.getUser();
//        LOGGER.info("---------------------------mycat---------------------------");
//        if (user != null) {
//          LOGGER.info("mycat id:{}  username:{}", mycat.sessionId(), user.getUserName());
//        } else {
//          LOGGER.info("mycat id:{}", mycat.sessionId());
//        }
//        ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
//        Queue<ByteBuffer> writeQueue = mycat.writeQueue();
//        LOGGER.info("byteBuffer:{} in proxyBuffer,writeQueue size:{}",
//            proxyBuffer.currentByteBuffer(), writeQueue);
//        boolean open = mycat.checkOpen();
//        LOGGER.info("open :{} hasClosed", open, mycat.hasClosed());
//        String schema = mycat.getSchema();
//        Charset charsetObject = mycat.charset();
//        String characterSetResults = mycat.getCharacterSetResults();
//        String charsetName = mycat.getCharsetName();
//        LOGGER.info("charsetName:{} charsetObject :{} characterSetResults:{} ", charsetName,
//            charsetObject, characterSetResults);
//        MySQLAutoCommit autoCommit = mycat.isAutoCommit();
//        MySQLIsolation isolation = mycat.getIsolation();
//        LOGGER.info("autoCommit :{} isolation :{}", autoCommit, isolation);
//        int lastErrorCode = mycat.getLastErrorCode();
//        String lastMessage = mycat.getLastMessage();
//        LOGGER.info("lastErrorCode :{} lastMessage:{}", lastErrorCode, lastMessage);
//        if (schema != null) {
//          LOGGER.info("schema :{}", schema);
//        }
//        String dataNode = mycat.getDafaultDatabase();
//        LOGGER.info("dataNode :{}", dataNode);
//        MySQLClientSession mySQLSession = mycat.getMySQLSession();
//        if (mySQLSession != null) {
//          LOGGER.info("backendId:{}", mySQLSession.sessionId());
//        }
//      }
//
//      MySQLSessionManager manager = thread.getMySQLSessionManager();
//      for (MySQLClientSession mysql : manager.getAllSessions()) {
//        LOGGER.info("---------------------------mysql---------------------------");
//        MySQLAutoCommit automCommit = mysql.isAutomCommit();
//        MySQLIsolation isolation = mysql.getIsolation();
//        LOGGER.info("automCommit:{} isolation:{}", automCommit, isolation);
//        String charset = mysql.getCharset();
//        String characterSetResult = mysql.getCharacterSetResult();
//        LOGGER.info("charset:{} characterSetResult:{}", charset, characterSetResult);
//
//        String lastMessage = mysql.getLastMessage();
//        LOGGER.info("lastMessage:{}", lastMessage);
//        MycatSession mycatSeesion = mysql.getMycatSeesion();
//        if (mycatSeesion != null) {
//          LOGGER.info("bindSource mycat session:{}", mycatSeesion.sessionId());
//        }
//        LOGGER.info("open:{} isClose:{}", mysql.checkOpen(), mysql.hasClosed());
//        ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
//        LOGGER.info("proxyBuffer:{}", proxyBuffer);
//        if (proxyBuffer != null) {
//          ByteBuffer byteBuffer = proxyBuffer.currentByteBuffer();
//          LOGGER.info("byteBuffer:{}", byteBuffer);
//        }
//        boolean idle = mysql.isIdle();
//        MySQLSessionMonopolizeType type = mysql.getMonopolizeType();
//        LOGGER.info("idle:{},monopolizeType:{}", idle, type);
//      }
//    }
//    Collection<MySQLDatasource> datasourceList = new ArrayList<>(runtime.getMySQLDatasourceList());
//    LOGGER.info("---------------------------datasource---------------------------");
//    for (MySQLDatasource datasource : datasourceList) {
//      String name = datasource.getName();
//      int sessionCounter = datasource.instance().getSessionCounter();
//      LOGGER.info("dataSourceName:{} sessionCounter:{}", name, sessionCounter);
//    }
//  }
//}