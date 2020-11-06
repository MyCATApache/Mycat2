package io.mycat.combinator;

import com.google.common.collect.ImmutableList;
import io.mycat.util.Pair;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.List;
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

    public static <T> Function<ByteBuffer, Pair<Predicate<CharBuffer>, T>> asByteBufferPredicateMap(List<Pair<String, T>> map, final Charset charset) {
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> charBufferPairFunction =
                asCharBufferPredicateMap(map);
        return byteBuffer -> charBufferPairFunction.apply(charset.decode(byteBuffer));
    }

    public static <T> Function<String, Pair<Predicate<CharBuffer>, T>> asStringPredicateMap(List<Pair<String, T>> map) {
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> charBufferPairFunction =
                asCharBufferPredicateMap(map);
        return s -> charBufferPairFunction.apply(CharBuffer.wrap(s));
    }
    public static <T> Function<char[], Pair<Predicate<CharBuffer>, T>> asCharArrayPredicateMap(List<Pair<String, T>> map) {
        Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> charBufferPairFunction =
                asCharBufferPredicateMap(map);
        return s -> charBufferPairFunction.apply(CharBuffer.wrap(s));
    }
    public static <T> Function<CharBuffer, Pair<Predicate<CharBuffer>, T>> asCharBufferPredicateMap(List<Pair<String, T>> list) {
        ImmutableList.Builder<Pair<Predicate<CharBuffer>, T>> builder = ImmutableList.builder();
        for (Pair<String, T> stringTEntry : list) {
            String key = stringTEntry.getKey();
            T value = stringTEntry.getValue();
            Predicate<CharBuffer> charBufferPredicate = asPredicateForCharBuffer(key);
            builder.add(Pair.of(charBufferPredicate, value));
        }
        ImmutableList<Pair<Predicate<CharBuffer>, T>> build = builder.build();
        return t -> {
            for (Pair<Predicate<CharBuffer>, T> predicateTPair : build) {
                if (predicateTPair.getKey().test(t)) {
                    return predicateTPair;
                }
            }
            return null;
        };
    }
}