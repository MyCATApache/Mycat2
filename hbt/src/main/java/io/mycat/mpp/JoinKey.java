/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package io.mycat.mpp;

import io.mycat.mpp.plan.DataAccessor;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * Selected key for join. It is a specialized projection, which must be Comparable
 *
 * @author eolivelli
 */
 public class JoinKey implements Comparable<JoinKey> {

    public static Function<DataAccessor, JoinKey> keyExtractor(int[] projection) {
        return (DataAccessor a) -> new JoinKey(a, projection);
    }

    private final DataAccessor dataAccessor;
    private final int[] selectedFields;

    public JoinKey(DataAccessor dataAccessor, int[] selectedFields) {
        this.dataAccessor = dataAccessor;
        this.selectedFields = selectedFields;
    }

    final private Object get(int i) {
        return dataAccessor.get(selectedFields[i]);
    }

    @Override
    public boolean equals(Object obj) {
        JoinKey da = (JoinKey) obj;
        int size = this.selectedFields.length;
        // leverage zero-copy and to not create temporary arrays
        for (int i = 0; i < size; i++) {
            final Comparable fromThis = (Comparable) get(i);
            final Comparable fromThat = (Comparable) da.get(i);
            if (fromThis.compareTo(fromThat) != 0) {
                return false;
            }
        }
        return true;
    }

    private int hashcode = Integer.MIN_VALUE;

    @Override
    public int hashCode() {
        if (hashcode == Integer.MIN_VALUE) {
            int size = this.selectedFields.length;
            int res = 0;
            // leverage zero-copy and to not create temporary arrays
            for (int i = 0; i < size; i++) {
                res += Objects.hashCode(get(i));
            }
            hashcode = res;
        }
        return hashcode;
    }

    @Override
    public int compareTo(JoinKey o) {
        JoinKey da = o;
        int size = this.selectedFields.length;
        // leverage zero-copy and to not create temporary arrays
        for (int i = 0; i < size; i++) {
            final Comparable fromThis = (Comparable) get(i);
            final Comparable fromThat = (Comparable) da.get(i);
            int res;
            if ((res = fromThis.compareTo(fromThat)) != 0) {
                return res;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return "RecordKey{" + "dataAccessor=" + dataAccessor + ", selectedFields=" + Arrays.toString(selectedFields) + '}';
    }

}
