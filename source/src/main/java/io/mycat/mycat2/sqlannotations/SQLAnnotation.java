package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by jamie on 2017/9/15.
 */
public interface SQLAnnotation extends Function<MycatSession, Boolean> {
   static final boolean isDebug=true;

   abstract public void init(Object args);


    public String getMethod();

    public void setMethod(String method);

}
