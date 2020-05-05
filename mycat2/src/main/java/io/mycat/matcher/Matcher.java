package io.mycat.matcher;

import io.mycat.util.Pair;

import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;

public interface Matcher<T>  {

    public static  interface Factory<T>{

        String getName();

        Matcher<T> create(List<Pair<String, T>> pairs,Pair<String, T> defaultPattern);
    }

    List<T> match(CharBuffer buffer, Map<String,Object> context);
}
