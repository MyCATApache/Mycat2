package io.mycat.combinator;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.CharBuffer;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class ReMatchersBenchmark {
    private static final Function<String, Pair<Predicate<CharBuffer>, String>> function;

    static {
        ImmutableList.Builder<Pair<String, String>> builder = ImmutableList.builder();
        add(builder, "select 1 from db1.travelrecord");
        add(builder, "select 2 from db1.travelrecord");
        ImmutableList<Pair<String, String>> pairs = builder.build();
        function = ReMatchers.asStringPredicateMap((List)pairs);
    }

    private static void add(ImmutableList.Builder<Pair<String, String>> builder, String s) {
        builder.add(Tuples.pair(s, s));
    }

    public ReMatchersBenchmark() {

    }

    @SneakyThrows
    public static void main(String[] args) {
        ReMatchersBenchmark reMatchersBenchmark = new ReMatchersBenchmark();
        reMatchersBenchmark.testMethod();
        Options opt = new OptionsBuilder().include(ReMatchersBenchmark.class.getSimpleName()).forks(1).warmupIterations(5)
                .measurementIterations(5).build();
        new Runner(opt).run();

    }

//    @Benchmark
    public void testMethod() {
        Pair<Predicate<CharBuffer>, String> apply = function.apply("select 2 from db1.travelrecord");
        if (apply == null){
            throw new RuntimeException();
        }
    }
}