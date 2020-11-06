package io.mycat.mpp.plan;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;

/**
 * row
 */
public interface DataAccessor{

    static DataAccessor of(Object[] values) {
        return new DataAccessorImpl(values);
    }

    Object get(int index);

    DataAccessor map(Object[] row);


}