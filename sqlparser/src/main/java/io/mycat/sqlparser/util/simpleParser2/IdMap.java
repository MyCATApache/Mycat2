package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.Map;

public class IdMap {
    byte[] word = new byte[64];
    int offset = 0;
    long hash = 0;
    Map<Long, String> ids = new HashMap<>();

    void append(byte b) {
        word[offset] = b;
        ++offset;
        if (offset < 9) {
            hash = hash << 4 | b;
        }
    }

    public boolean isKeyword() {
        String s = ids.get(hash);
        if (s != null) {
            for (int i = 8; i < offset; i++) {
                if (s.charAt(i) != word[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}