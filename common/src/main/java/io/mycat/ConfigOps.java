package io.mycat;

public interface ConfigOps extends AutoCloseable{

    Object  currentConfig();

    void commit(Object ops);

    void close();
}