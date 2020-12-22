package io.mycat.hint;

import io.mycat.config.SqlCacheConfig;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;

public class CreateSqlCacheHint extends HintBuilder {
    private SqlCacheConfig config;

    public static String create(SqlCacheConfig config) {
        CreateSqlCacheHint createSqlCacheHint = new CreateSqlCacheHint();
        createSqlCacheHint.setSqlCacheConfig(config);
        return createSqlCacheHint.build();
    }

    public static String createDefault() {
        return create(new SqlCacheConfig());
    }

    public void setSqlCacheConfig(SqlCacheConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "createSqlCache";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}