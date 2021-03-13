package io.mycat.replica;

public interface SessionCounterProvider {
    int getSessionCounter(String name);
}
