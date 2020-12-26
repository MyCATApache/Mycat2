package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.MySQLPayloadReadView;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.handler.front.MySQLClientAuthHandler;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author sneaky
 * @since 1.0.0
 */
public class MysqlProxyServer {
    private static final Logger logger = LoggerFactory.getLogger(MysqlProxyServer.class);
    public static MySQLClientManager mySQLClientManager;
    static enum State {
        HEAD,
        PAYLOAD
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new MysqlProxyServerVerticle());
    }

    public static class MysqlProxyServerVerticle extends AbstractVerticle {
        private final int port = 3308;
        private final String mysqlHost = "127.0.0.1";
        public static final AtomicInteger sessionId = new AtomicInteger();



        @Override
        public void start() throws Exception {
            if (mySQLClientManager == null) {
                mySQLClientManager = new MySQLClientManager(this.vertx);
            }
            NetServer netServer = vertx.createNetServer();//创建代理服务器
            netServer.connectHandler((socket)->new VertxMySQLAuthHandler(
                    socket,null,new VertxMySQLHandler()
            )).listen(port, listenResult -> {//代理服务器的监听端口
                if (listenResult.succeeded()) {
                    //成功启动代理服务器
                    logger.info("Mysql proxy server start up.");
                } else {
                    //启动代理服务器失败
                    logger.error("Mysql proxy exit. because: " + listenResult.cause().getMessage(), listenResult.cause());
                    System.exit(1);
                }
            });
        }
    }

    public interface MycatSession {

    }



    public static class MysqlProxyConnection implements MycatSession {
        private final NetSocket clientSocket;
        private final NetSocket serverSocket;

        public MysqlProxyConnection(NetSocket clientSocket, NetSocket serverSocket) {
            this.clientSocket = clientSocket;
            this.serverSocket = serverSocket;
        }

        private void proxy() {
            //当代理与mysql服务器连接关闭时，关闭client与代理的连接
            serverSocket.closeHandler(v -> clientSocket.close());
            //反之亦然
            clientSocket.closeHandler(v -> serverSocket.close());
            //不管那端的连接出现异常时，关闭两端的连接
            serverSocket.exceptionHandler(e -> {
                logger.error(e.getMessage(), e);
                close();
            });
            clientSocket.exceptionHandler(e -> {
                logger.error(e.getMessage(), e);
                close();
            });
            //当收到来自客户端的数据包时，转发给mysql目标服务器
            clientSocket.handler(buffer -> serverSocket.write(buffer));
            //当收到来自mysql目标服务器的数据包时，转发给客户端
            serverSocket.handler(buffer -> clientSocket.write(buffer));
        }

        private void close() {
            clientSocket.close();
            serverSocket.close();
        }
    }
}