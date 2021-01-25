package org.apache.calcite.util;

import com.google.common.collect.ImmutableMap;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.NewMycatDataContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;

public enum RxBuiltInMethod {
    OBSERVABLE_SELECT(RxBuiltInMethodImpl.class,
            "select", Observable.class, Function1.class),
    OBSERVABLE_FILTER(RxBuiltInMethodImpl.class,
            "filter", Observable.class, Predicate1.class),
    OBSERVABLE_UNION_ALL(RxBuiltInMethodImpl.class, "unionAll", Observable.class, Observable.class),
    OBSERVABLE_TOP_N(RxBuiltInMethodImpl.class, "topN", Observable.class, Comparator.class, long.class, long.class),
    OBSERVABLE_SORT(RxBuiltInMethodImpl.class, "sort", Observable.class, Comparator.class),
    OBSERVABLE_LIMIT(RxBuiltInMethodImpl.class, "limit", Observable.class, long.class),
    OBSERVABLE_OFFSET(RxBuiltInMethodImpl.class, "offset", Observable.class, long.class),
    TO_ENUMERABLE(RxBuiltInMethodImpl.class, "toEnumerable", Object.class),
    TO_OBSERVABLE(RxBuiltInMethodImpl.class, "toObservable", Object.class),
    OBSERVABLE_MERGE_SORT(RxBuiltInMethodImpl.class, "mergeSort", List.class, Comparator.class, long.class, long.class),
    OBSERVABLE_MERGE_SORT2(RxBuiltInMethodImpl.class, "mergeSort", List.class, Comparator.class),
    OBSERVABLE_MATIERIAL(RxBuiltInMethodImpl.class, "matierial", Observable.class),
    OBSERVABLE_BIND(Bindable.class, "bindObservable", NewMycatDataContext.class),
    AS_OBSERVABLE(RxBuiltInMethodImpl.class, "asObservable", Object[][].class),

    ;
    public final Method method;
    public final Constructor constructor;
    public final Field field;

    public static final ImmutableMap<Method, RxBuiltInMethod> MAP;

    static {

        final ImmutableMap.Builder<Method, RxBuiltInMethod> builder =
                ImmutableMap.builder();
        try {
            for (RxBuiltInMethod value : RxBuiltInMethod.values()) {
                if (value.method != null) {
                    builder.put(value.method, value);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
        MAP = builder.build();
    }

    RxBuiltInMethod(Method method, Constructor constructor, Field field) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }

    /**
     * Defines a method.
     */
    RxBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /**
     * Defines a constructor.
     */
    RxBuiltInMethod(Class clazz, Class... argumentTypes) {
        this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /**
     * Defines a field.
     */
    RxBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
        this(null, null, Types.lookupField(clazz, fieldName));
        assert dummy : "dummy value for method overloading must be true";
    }

    public String getMethodName() {
        return method.getName();
    }
}
