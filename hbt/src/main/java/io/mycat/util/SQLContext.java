package io.mycat.util;

import io.mycat.upondb.MycatDBContext;

import java.util.Collections;
import java.util.Map;

public interface SQLContext {

    MycatDBContext getMycatDBContext();

    Object getSQLVariantRef(String target);

    Map<String, Object> getParameters();

    void setParameters(Map<String, Object> parameters);

    Map<String, MySQLFunction> functions();

    String getDefaultSchema();

    default void clearParameters() {
        setParameters(Collections.emptyMap());
    }

    void setDefaultSchema(String simpleName);

   default String simplySql(String explain){
       return explain;
   }

    long lastInsertId();
}