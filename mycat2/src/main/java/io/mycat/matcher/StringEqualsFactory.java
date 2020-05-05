package io.mycat.matcher;

import io.mycat.util.Pair;

import java.nio.CharBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StringEqualsFactory<T> implements Matcher.Factory<T> {

    @Override
    public String getName() {
        return "equals";
    }

    @Override
    public Matcher<T> create(List<Pair<String, T>> pairs, Pair<String, T> defaultPattern) {
        Map<CharBuffer, T> map = pairs.stream().sequential().collect(Collectors.toMap(k -> CharBuffer.wrap(k.getKey()), v -> v.getValue()));
        return (buffer, context) -> {
            T t1 = map.get(buffer);
            if (t1 != null) {
                return Collections.singletonList(t1);
            }
            return Collections.singletonList(defaultPattern.getValue());
        };
    }
}