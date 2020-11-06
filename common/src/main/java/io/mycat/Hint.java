package io.mycat;

import java.util.Map;

public interface Hint {
    String getName();
    void accept(String buffer, Map<String, Object> t);
}