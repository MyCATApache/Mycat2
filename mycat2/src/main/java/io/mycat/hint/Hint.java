package io.mycat.hint;

import java.nio.CharBuffer;
import java.util.Map;


public interface Hint {
    String getName();
    void accept(CharBuffer buffer, Map<String, Object> t);
}