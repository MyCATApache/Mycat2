package io.mycat.mpp.plan;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface Scanner extends Iterator<DataAccessor> {
    public default Stream<DataAccessor> stream() {
        Iterable<DataAccessor> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static Scanner of(Iterable<DataAccessor> iterable) {
        return of(iterable.iterator());
    }

    public static Scanner of(Stream<DataAccessor> iterable) {
        return of(iterable.iterator());
    }

    public static Scanner of(Iterator<DataAccessor> iterator) {
        return new Scanner() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public DataAccessor next() {
                return iterator.next();
            }
        };
    }

    public static Scanner of(Generator p) {
        return new Scanner() {
            Optional<DataAccessor> row = Optional.empty();

            @Override
            public boolean hasNext() {
                this.row = p.next();
                return this.row.isPresent();
            }

            @Override
            public DataAccessor next() {
                return this.row.get();
            }
        };
    }
    public default Generator generator(){
        return () -> {
            boolean b = Scanner.this.hasNext();
            if (!b)return Optional.empty();
            return  Optional.of(Scanner.this.next());
        };
    }
}