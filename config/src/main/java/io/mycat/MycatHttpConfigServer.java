package io.mycat;

import com.sun.net.httpserver.HttpServer;
import io.mycat.util.JsonUtil;
import io.mycat.util.YamlUtil;
import org.apache.log4j.lf5.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;


public enum MycatHttpConfigServer {
    INSTANCE;
    static final Logger logger = LoggerFactory.getLogger(MycatHttpConfigServer.class);
    private MycatConfig config;
    private Map<String, Object> globalVariables;

    public void start() throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(8082), 0);
        httpServer.createContext("/mycat/config", httpExchange -> {
            byte[] respContents = YamlUtil.dump(config).getBytes();
            try {
                httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
                httpExchange.sendResponseHeaders(200, respContents.length);
                httpExchange.getResponseBody().write(respContents);
            }finally {
                httpExchange.close();
            }
        });
        httpServer.createContext("/mycat/globalVariables", httpExchange -> {
            byte[] respContents = JsonUtil.toJson(globalVariables).getBytes();
            try {
                httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
                httpExchange.sendResponseHeaders(200, respContents.length);
                httpExchange.getResponseBody().write(respContents);
            }finally {
                httpExchange.close();
            }
        });
        httpServer.createContext("/mycat/reportReplica", httpExchange -> {
            String text = new String(StreamUtils.getBytes(httpExchange.getRequestBody()));
            logger.info("reportReplica");
            logger.info(text);
            byte[] respContents = new byte[]{};
            try {
                httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
                httpExchange.sendResponseHeaders(200, respContents.length);
                httpExchange.getResponseBody().write(respContents);
            }finally {
                httpExchange.close();
            }
        });
        httpServer.createContext("/mycat/reportConfig", httpExchange -> {
            String text = new String(StreamUtils.getBytes(httpExchange.getRequestBody()));
            logger.info("reportConfig");
            logger.info(text);
            byte[] respContents = new byte[]{};
            try {
                httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
                httpExchange.sendResponseHeaders(200, respContents.length);
                httpExchange.getResponseBody().write(respContents);
            }finally {
                httpExchange.close();
            }
        });
        httpServer.start();
    }

    public void setConfig(MycatConfig config) {
        this.config = config;
    }

    public void setGlobalVariables(Map<String, Object> globalVariables) {
        this.globalVariables = globalVariables;
    }
}