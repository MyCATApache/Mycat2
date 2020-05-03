package io.mycat.client;

import io.mycat.combinator.ReMatchers;
import io.mycat.util.Pair;

import java.nio.CharBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public class RobPikeMatcherFactory<T> implements Matcher.Factory<T> {
    @Override
    public String getName() {
        return "Rob Pike";
    }

    @Override
    public Matcher<T> create(List<Pair<String, T>> pairs, Pair<String, T> defaultPattern) {
        Objects.requireNonNull(pairs);
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> function = ReMatchers.asCharBufferPredicateMap(pairs);
        return (buffer,c) -> {
            Pair<Predicate<CharBuffer>, T> apply = function.apply(buffer);
            if (apply == null) {
                if (defaultPattern != null) {
                    return Collections.singletonList(defaultPattern.getValue());
                } else {
                    return null;
                }
            }
            return Collections.singletonList(apply.getValue());
        };
    }
}