package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Created by jamie on 2017/9/15.
 */
public abstract class SQLAnnotation {
   static final boolean isDebug=true;
   String method;
   abstract public void init(Map<String,String> args);
   abstract public BufferSQLContext apply(BufferSQLContext t);

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }
    Map<String,String> args;

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    @Override
    public String toString() {
        return "SQLAnnotation{" +
                "method='" + method + '\'' +
                ", args=" + args +
                '}';
    }
}
