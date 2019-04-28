package io.mycat.proxy;

public class MycatExpection extends RuntimeException {
    public MycatExpection(String message) {
        super(message);
    }

    public MycatExpection(String message, Throwable cause) {
        super(message, cause);
    }
}
