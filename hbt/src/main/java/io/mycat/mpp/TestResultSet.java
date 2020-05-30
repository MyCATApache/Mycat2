package io.mycat.mpp;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class TestResultSet {
    public static void check(Iterator<Object[]> iterator, Iterator<Object[]> iterator2) {
        Class[] types = null;
        while (iterator.hasNext() && iterator2.hasNext()) {
            Object[] next = Objects.requireNonNull(iterator.next());
            Object[] next2 = iterator2.next();
            //init
            if (types == null && next != null) {
                types = new Class[next.length];
            }

            //assign type
            if (types != null) {
                for (int i = 0; i < types.length; i++) {
                    //init
                    if (types[i] == null && next[i] != null) {
                        types[i] = next[i].getClass();
                    }
                    //check
                    if (types[i] != null && next[i] != null) {
                        if (!types[i].equals(next[i].getClass())) {
                            throw new AssertionError();
                        }
                    }
                }
            }

            if (Arrays.equals(next, next2)) {
                throw new AssertionError();
            }
        }
    }
}