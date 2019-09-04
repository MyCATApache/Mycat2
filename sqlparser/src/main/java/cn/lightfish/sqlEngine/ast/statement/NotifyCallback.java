package cn.lightfish.sqlEngine.ast.statement;

public interface NotifyCallback {
    void useSchema(String schema);

    void setAutocommit(boolean b);

    void setNamesCharset(String value);

    void setCharacterSetResultsCharset(String value);

    void setSessionIsolationLevel(String isolationLevel);
}