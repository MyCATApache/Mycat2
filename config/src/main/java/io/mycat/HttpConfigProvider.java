//package io.mycat;
//
//import io.mycat.util.JsonUtil;
//import io.mycat.util.YamlUtil;
//import lombok.SneakyThrows;
//import okhttp3.OkHttpClient;
//import okhttp3.Request;
//import okhttp3.RequestBody;
//import okhttp3.Response;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class HttpConfigProvider implements ConfigProvider {
//    static final Logger logger = LoggerFactory.getLogger(HttpConfigProvider.class);
//    private String mycatconfigUrl;
//    private MycatConfig config;
//    private String globalVariablesUrl;
//    private Map globalVariables;
//    private String reportReplicaUrl;
//    private String reportConfigUrl;
//
//    @Override
//    public synchronized void create(Class rootClass, Map<String, String> config) throws Exception {
//        this.mycatconfigUrl = config.getOrDefault("configUrl", "http://127.0.0.1:8082/mycat/config");
//        this.globalVariablesUrl = config.getOrDefault("globalVariablesUrl", "http://127.0.0.1:8082/mycat/globalVariables");
//        this.reportReplicaUrl = config.getOrDefault("reportReplicaUrl", "http://127.0.0.1:8082/mycat/reportReplica");
//        this.reportConfigUrl = config.getOrDefault("reportConfigUrl", "http://127.0.0.1:8082/mycat/reportConfig");
//        try {
//            this.config = YamlUtil.loadText(requestByURL(this.mycatconfigUrl), MycatConfig.class);
//            this.globalVariables = JsonUtil.from(requestByURL(this.globalVariablesUrl), Map.class);
//        } catch (Throwable e) {
//            logger.error("fetch default url config fail", e);
//        }
//    }
//
//    private static String requestByURL(String url) throws IOException {
//        String context;
//        OkHttpClient client = new OkHttpClient();
//        Request request = new Request.Builder()
//                .url(url)
//                .build();
//        try (Response response = client.newCall(request).execute()) {
//            context = response.body().string();
//        }
//        return context;
//    }
//
//    @Override
//    public void fetchConfig(String path) throws Exception {
//        this.config = YamlUtil.loadText(requestByURL(path), MycatConfig.class);
//        this.globalVariables = JsonUtil.from(requestByURL(this.globalVariablesUrl), Map.class);
//    }
//
//    @Override
//    public void fetchConfig() throws Exception {
//        fetchConfig(mycatconfigUrl);
//    }
//
//    @Override
//    public void report(MycatConfig changed) {
//        HashMap<String, Object> body = new HashMap<>();
//        body.put("config", changed);
//        String bodyText = JsonUtil.toJson(body);
//        OkHttpClient client = new OkHttpClient();
//        Request request = new Request.Builder()
//                .url(reportConfigUrl)
//                .post(RequestBody.create(bodyText.getBytes()))
//                .build();
//        try (Response response = client.newCall(request).execute()) {
//
//        } catch (Throwable e) {
//            logger.error("reportReplica:" + bodyText, e);
//        }
//    }
//
//    @Override
//    public MycatConfig currentConfig() {
//        return config;
//    }
//
//    @Override
//    public Map<String, Object> globalVariables() {
//        return globalVariables;
//    }
//
//    @Override
//    @SneakyThrows
//    public void reportReplica(String replicaName, List<String> dataSourceList) {
//        HashMap<String, Object> body = new HashMap<>();
//        body.put("replicaName", replicaName);
//        body.put("dataSourceList", dataSourceList);
//        String bodyText = JsonUtil.toJson(body);
//        OkHttpClient client = new OkHttpClient();
//        Request request = new Request.Builder()
//                .url(reportReplicaUrl)
//                .post(RequestBody.create(bodyText.getBytes()))
//                .build();
//        try (Response response = client.newCall(request).execute()) {
//
//        } catch (Throwable e) {
//            logger.error("reportReplica:" + bodyText, e);
//        }
//    }
//}