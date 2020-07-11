package io.mycat.hbt4;

import io.mycat.hbt3.Part;

public interface DatasourceFactory extends AutoCloseable{
    Executor create(Part... parts);
}