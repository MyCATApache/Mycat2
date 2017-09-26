package io.mycat.mycat2.sqlannotations;

import java.util.function.Function;

import io.mycat.mycat2.MycatSession;

/**
 * Created by jamie on 2017/9/15.
 */
public interface SQLAnnotation extends Function<MycatSession, Boolean> {

   abstract public void init(Object args);


    public String getMethod();

    public void setMethod(String method);

}
