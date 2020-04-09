package io.mycat.combinator;

import com.google.common.collect.ImmutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class ReMatchers {

    public static Predicate<CharBuffer> asPredicateForCharBuffer(String regxp) {
        ReMatcherImpl reMatch = new ReMatcherImpl(regxp);
        return s -> reMatch.match(s);
    }

    public static Predicate<String> asPredicateForString(String regxp) {
        Predicate<CharBuffer> charBufferPredicate = asPredicateForCharBuffer(regxp);
        return s -> charBufferPredicate.test(CharBuffer.wrap(s));
    }

    public static Predicate<char[]> asPredicateForCharArray(String regxp) {
        Predicate<CharBuffer> charBufferPredicate = asPredicateForCharBuffer(regxp);
        return s -> charBufferPredicate.test(CharBuffer.wrap(s));
    }

    public static Predicate<ByteBuffer> asPredicateForCharsetBytebuffer(String regxp, final Charset charset) {
        Predicate<CharBuffer> charBufferPredicate = asPredicateForCharBuffer(regxp);
        return s -> charBufferPredicate.test(charset.decode(s));
    }

    public static <T> Function<ByteBuffer, Pair<Predicate<CharBuffer>, T>> asByteBufferPredicateMap(Map<String, T> map, final Charset charset) {
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> charBufferPairFunction =
                asCharBufferPredicateMap(map);
        return byteBuffer -> charBufferPairFunction.apply(charset.decode(byteBuffer));
    }

    public static <T> Function<String, Pair<Predicate<CharBuffer>, T>> asStringPredicateMap(Map<String, T> map) {
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> charBufferPairFunction =
                asCharBufferPredicateMap(map);
        return s -> charBufferPairFunction.apply(CharBuffer.wrap(s));
    }
    public static <T> Function<char[], Pair<Predicate<CharBuffer>, T>> asCharArrayPredicateMap(Map<String, T> map) {
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> charBufferPairFunction =
                asCharBufferPredicateMap(map);
        return s -> charBufferPairFunction.apply(CharBuffer.wrap(s));
    }
    public static <T> Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> asCharBufferPredicateMap(Map<String, T> map) {
        ImmutableList.Builder<Pair<Predicate<CharBuffer>, T>> builder = ImmutableList.builder();
        for (Map.Entry<String, T> stringTEntry : map.entrySet()) {
            String key = stringTEntry.getKey();
            T value = stringTEntry.getValue();
            Predicate<CharBuffer> charBufferPredicate = asPredicateForCharBuffer(key);
            builder.add(Tuples.pair(charBufferPredicate, value));
        }
        ImmutableList<Pair<Predicate<CharBuffer>, T>> build = builder.build();
        return t -> {
            for (Pair<Predicate<CharBuffer>, T> predicateTPair : build) {
                if (predicateTPair.getOne().test(t)) {
                    return predicateTPair;
                }
            }
            return null;
        };
    }
}