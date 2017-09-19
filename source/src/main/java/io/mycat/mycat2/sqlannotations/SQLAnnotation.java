package io.mycat.mycat2.sqlannotations;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Created by jamie on 2017/9/15.
 */
public interface SQLAnnotation<T> extends Function<T, T> {
    boolean isDebug=true;
    void init(Map<String,String> args);
}
