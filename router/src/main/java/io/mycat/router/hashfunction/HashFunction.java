package io.mycat.router.hashfunction;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface HashFunction {
    static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    static final byte[] DEFAULT_KEY = "".getBytes(DEFAULT_CHARSET);
    default long hash(String key) {
        if (key == null) {
           return hash(DEFAULT_KEY);
        }
        return hash(key.getBytes(DEFAULT_CHARSET));
    }

    long hash(byte[] bytes);
}