package io.mycat.hint;

import io.mycat.config.DatasourceConfig;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;

import java.net.URL;
import java.text.MessageFormat;
import java.util.Map;

public class CreateDataSourceHint extends HintBuilder {
    private DatasourceConfig config;

    public static String create(DatasourceConfig config) {
        CreateDataSourceHint createDataSourceHint = new CreateDataSourceHint();
        createDataSourceHint.setDatasourceConfig(config);
        return createDataSourceHint.build();
    }

    public static String create(
            String name,
            String url
    ) {
        return create(name, "root", "123456", url);
    }

    public static String create(
            String name,
            String user,
            String password,
            String url
    ) {
        DatasourceConfig datasourceConfig = new DatasourceConfig();
        datasourceConfig.setName(name);
        datasourceConfig.setUrl(url);
        datasourceConfig.setPassword(password);
        datasourceConfig.setUser(user);
        datasourceConfig.setPassword(password);
        return create(datasourceConfig);
    }

    public void setDatasourceConfig(DatasourceConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "createDataSource";
    }

    @SneakyThrows
    @Override
    public String build() {
        String urlStr = config.getUrl();

        Map<String, String> urlParameters = JsonUtil.urlSplit(urlStr);
        String username = urlParameters.get("username");
        String password = urlParameters.get("password");
        if(password != null){
            config.setPassword(password);
        }
        if(username != null){
            config.setUser(username);
        }
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }


}