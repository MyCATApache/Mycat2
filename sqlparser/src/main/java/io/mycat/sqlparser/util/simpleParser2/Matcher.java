package io.mycat.sqlparser.util.simpleParser2;

import java.util.Map;

public interface Matcher {
        boolean accept(Seq token);
        public void reset();
        public boolean acceptAll();
        public int id();
        public Map<String,Position> context();
    }
