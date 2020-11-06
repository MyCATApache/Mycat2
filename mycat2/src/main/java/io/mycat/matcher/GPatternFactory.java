package io.mycat.matcher;

import io.mycat.pattern.GPattern;
import io.mycat.pattern.GPatternBuilder;
import io.mycat.pattern.GPatternMatcher;
import io.mycat.util.Pair;

import java.util.*;

/**
 * @author Junwen Chen
 **/
public class GPatternFactory<T> implements Matcher.Factory<T> {
    @Override
    public String getName() {
        return "GPattern";
    }

    @Override
    public Matcher<T> create(List<Pair<String, T>> pairs, T defaultPattern) {
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
            T t1 = null;
            GPatternMatcher matcher = groupPattern.matcher(buffer.toString());
            if (matcher.acceptAll()) {
                matcher.namesContext((Map) context);
                t1 = map.get(matcher.id());
            }
            if (t1 == null) {
                if (defaultPattern != null) {
                    return Collections.singletonList(defaultPattern);
                } else {
                    return Collections.emptyList();
                }
            }
            if (defaultPattern != null) {
                return (List) Arrays.asList(t1,defaultPattern);
            } else {
                return (List)Collections.singletonList(t1);
            }
        };
    }
}