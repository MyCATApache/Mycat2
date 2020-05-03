package io.mycat.client;

import io.mycat.pattern.GPattern;
import io.mycat.pattern.GPatternBuilder;
import io.mycat.pattern.GPatternMatcher;
import io.mycat.util.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GPatternFactory<T> implements Matcher.Factory<T> {
    @Override
    public String getName() {
        return "GPattern";
    }

    @Override
    public Matcher<T> create(List<Pair<String, T>> pairs, Pair<String, T> defaultPattern) {
        GPatternBuilder gPatternBuilder = new GPatternBuilder();
        Map<Integer, T> map = new HashMap<>();
        for (Pair<String, T> pair : pairs) {
            Integer i = gPatternBuilder.addRule(pair.getKey());
            if (!map.containsKey(i)) {
                map.put(i, pair.getValue());
            } else {
                throw new IllegalArgumentException("duplicate pattern:" + pair);
            }
        }
        GPattern groupPattern = gPatternBuilder.createGroupPattern();

        return (buffer, context) -> {
            GPatternMatcher matcher = groupPattern.matcher(buffer.toString());
            if (matcher.acceptAll()) {
                matcher.namesContext((Map) context);
                return Collections.singletonList(map.get(matcher.id()));
            }
            return Collections.singletonList(defaultPattern.getValue());
        };
    }
}