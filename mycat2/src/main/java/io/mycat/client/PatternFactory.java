package io.mycat.client;

import io.mycat.util.Pair;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PatternFactory<T> implements Matcher.Factory<T> {

    @Override
    public Matcher<T> create(List<Pair<String, T>> pairs, Pair<String, T> defaultPattern) {
        final List<Pattern> patterns = pairs.stream()
                .map(pair -> Pattern.compile(pair.getKey()))
                .collect(Collectors.toList());
        return new Matcher<T>() {
            @Override
            public List<T> match(CharBuffer buffer, Map<String, Object> context) {
                int size = patterns.size();
                List<T> list = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    Pattern pattern = patterns.get(i);
                    if (pattern.matcher(buffer).find()) {
                        list.add(pairs.get(i).getValue());
                    }
                }
                return list;
            }
        };
    }
}