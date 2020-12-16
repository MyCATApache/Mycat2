package io.mycat.hint;

import io.mycat.util.JsonUtil;

import java.text.MessageFormat;

public class SetUserDialectHint extends HintBuilder {
    private String dialect;
    private String username;

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDialect() {
        return dialect;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public String getCmd() {
        return "setUserDialect";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(this));
    }

    public static String create(
             String dialect,
             String username
    ) {
        SetUserDialectHint createDataSourceHint = new SetUserDialectHint();
        createDataSourceHint.setUsername(username);
        createDataSourceHint.setDialect(dialect);
        return createDataSourceHint.build();
    }

}