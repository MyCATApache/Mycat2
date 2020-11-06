package io.mycat.matcher;

import io.mycat.util.Pair;

import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;

/**
 * @author Junwen Chen
 **/
public interface Matcher<T> {

    public static interface Factory<T> {

        String getName();

        Matcher<T> create(List<Pair<String, T>> pairs, T defaultPattern);
    }

    List<T> match(CharBuffer buffer, Map<String, Object> context);
}
