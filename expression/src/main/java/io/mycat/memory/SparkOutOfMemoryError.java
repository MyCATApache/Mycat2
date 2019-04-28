package io.mycat.memory;

public final class SparkOutOfMemoryError extends OutOfMemoryError {

    public SparkOutOfMemoryError(String s) {
        super(s);
    }

    public SparkOutOfMemoryError(OutOfMemoryError e) {
        super(e.getMessage());
    }
}