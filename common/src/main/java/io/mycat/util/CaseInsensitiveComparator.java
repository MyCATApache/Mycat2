/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.util;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * Comparator that compares all strings differently, but if two strings are
 * equal in case-insensitive match they are right next to each other.
 *
 * <p>Note: strings that differ only in upper-lower case are treated by this comparator
 * as distinct.
 *
 * <p>In a collection sorted on this comparator, we can find case-insensitive matches
 * for a given string using
 * {@link #floorKey(String)}
 * and {@link #ceilingKey(String)}.
 */
class CaseInsensitiveComparator implements Comparator {
    static final CaseInsensitiveComparator COMPARATOR = new CaseInsensitiveComparator();

    /**
     * Enables to create floor and ceiling keys for given string.
     */
    private static final class Key implements java.lang.Comparable {
        public final String value;
        public final int compareResult;

        private Key(String value, int compareResult) {
            this.value = value;
            this.compareResult = compareResult;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public int compareTo(@NotNull Object o) {
            return innerCompare(this, o);
        }
    }

    Object floorKey(String key) {
        return new Key(key, -1);
    }

    Object ceilingKey(String key) {
        return new Key(key, 1);
    }

    @Override
    public int compare(Object o1, Object o2) {
        return innerCompare(o1, o2);
    }

    public static int innerCompare(Object o1, Object o2) {
        String s1 = o1.toString();
        String s2 = o2.toString();
        int c = s1.compareToIgnoreCase(s2);
        if (c != 0) {
            return c;
        }
        if (o1 instanceof Key) {
            return ((Key) o1).compareResult;
        }
        if (o2 instanceof Key) {
            return -((Key) o2).compareResult;
        }
        return s1.compareTo(s2);
    }
}
