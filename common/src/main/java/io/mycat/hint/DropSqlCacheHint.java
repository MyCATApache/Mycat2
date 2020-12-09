package io.mycat.hint;

import io.mycat.config.SqlCacheConfig;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;

public class DropSqlCacheHint extends HintBuilder {
    private SqlCacheConfig config;

    public void setSqlCacheConfig(SqlCacheConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "dropSqlCache";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }

    public static String create(String dropName) {
        DropSqlCacheHint createSqlCacheHint = new DropSqlCacheHint();
        SqlCacheConfig sqlCacheConfig = new SqlCacheConfig();
        sqlCacheConfig.setName(dropName);
        createSqlCacheHint.setSqlCacheConfig(sqlCacheConfig);
        return createSqlCacheHint.build();
    }

}