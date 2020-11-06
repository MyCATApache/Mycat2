package io.mycat.matcher;

import io.mycat.util.Pair;

import java.nio.CharBuffer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
/**
 * @author Junwen Chen
 **/
public class PatternFactory<T> implements Matcher.Factory<T> {

    @Override
    public String getName() {
        return "JSR-51";
    }

    @Override
    public Matcher<T> create(List<Pair<String, T>> pairs,T defaultPattern) {
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
                        list.add(Objects.requireNonNull(pairs.get(i).getValue()));
                    }
                }
                if (list.isEmpty()) {
                    if (defaultPattern != null) {
                        return Collections.singletonList(defaultPattern);
                    } else {
                        return Collections.emptyList();
                    }
                }
                if (defaultPattern != null) {
                    list.add(defaultPattern);
                    return list;
                } else {
                    return list;
                }
            }
        };
    }
}