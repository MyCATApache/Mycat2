package io.mycat;

public interface VertxSession {
    int getCapabilities();

    MycatDataContext getDataContext();

    void close();

    void sendOk();

    void writeError(Throwable throwable);
}
