package io.mycat.api.collector;

import java.io.Closeable;

public interface BaseIterator extends Closeable {

    boolean next();

    void close();

    boolean wasNull();
}
