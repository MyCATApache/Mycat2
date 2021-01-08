package org.apache.calcite.runtime;

import lombok.Getter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Getter
public class CodeExecuterContext {
    private List<RelNode> mycatViews;
    final Map<String, Object> context;
    final ArrayBindable bindable;
    final String code;

    public CodeExecuterContext(List<RelNode> mycatViews, Map<String, Object> context, ArrayBindable bindable, String code) {
        this.mycatViews = mycatViews;
        this.context = context;
        this.bindable = bindable;

//                new ArrayBindable() {
//            @Override
//            public Class<Object[]> getElementType() {
//                return Object[].class;
//            }
//
//            @Override
//            public Enumerable<Object[]> bind(NewMycatDataContext dataContext) {
//                return CodeExecuterContext.bind(dataContext);
//            }
//        };
        this.code = code;
    }

    public static final CodeExecuterContext of(List<RelNode> mycatViews, Map<String, Object> context,
                                               ArrayBindable bindable,
                                               String code) {
        return new CodeExecuterContext(mycatViews,context, bindable, code);
    }

    public static org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.runtime.NewMycatDataContext root) {
        final org.apache.calcite.rel.RelNode v1stashed = (org.apache.calcite.rel.RelNode) root.get("v1stashed");
        final org.apache.calcite.rel.RelNode v0stashed = (org.apache.calcite.rel.RelNode) root.get("v0stashed");
        final org.apache.calcite.rel.RelNode v3stashed = (org.apache.calcite.rel.RelNode) root.get("v3stashed");
        final org.apache.calcite.rel.RelNode v2stashed = (org.apache.calcite.rel.RelNode) root.get("v2stashed");
        final org.apache.calcite.rel.RelNode v4stashed = (org.apache.calcite.rel.RelNode) root.get("v4stashed");
        final org.apache.calcite.linq4j.Enumerable _inputEnumerable = org.apache.calcite.linq4j.EnumerableDefaults.nestedLoopJoin(root.getEnumerable(v0stashed), root.getEnumerable(v1stashed), new org.apache.calcite.linq4j.function.Predicate2() {
                    public boolean apply(Object[] left, Object[] right) {
                        return true;
                    }
                    public boolean apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , new org.apache.calcite.linq4j.function.Function2() {
                    public Object[] apply(Object[] left, Object[] right) {
                        return new Object[] {
                                left[0],
                                left[1],
                                left[2],
                                left[3],
                                left[4],
                                left[5],
                                right[0],
                                right[1],
                                right[2]};
                    }
                    public Object[] apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , org.apache.calcite.linq4j.JoinType.INNER).concat(org.apache.calcite.linq4j.EnumerableDefaults.nestedLoopJoin(root.getEnumerable(v2stashed), root.getEnumerable(v1stashed), new org.apache.calcite.linq4j.function.Predicate2() {
                    public boolean apply(Object[] left, Object[] right) {
                        return true;
                    }
                    public boolean apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , new org.apache.calcite.linq4j.function.Function2() {
                    public Object[] apply(Object[] left, Object[] right) {
                        return new Object[] {
                                left[0],
                                left[1],
                                left[2],
                                left[3],
                                left[4],
                                left[5],
                                right[0],
                                right[1],
                                right[2]};
                    }
                    public Object[] apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , org.apache.calcite.linq4j.JoinType.INNER)).concat(org.apache.calcite.linq4j.EnumerableDefaults.nestedLoopJoin(root.getEnumerable(v3stashed), root.getEnumerable(v1stashed), new org.apache.calcite.linq4j.function.Predicate2() {
                    public boolean apply(Object[] left, Object[] right) {
                        return true;
                    }
                    public boolean apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , new org.apache.calcite.linq4j.function.Function2() {
                    public Object[] apply(Object[] left, Object[] right) {
                        return new Object[] {
                                left[0],
                                left[1],
                                left[2],
                                left[3],
                                left[4],
                                left[5],
                                right[0],
                                right[1],
                                right[2]};
                    }
                    public Object[] apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , org.apache.calcite.linq4j.JoinType.INNER)).concat(org.apache.calcite.linq4j.EnumerableDefaults.nestedLoopJoin(root.getEnumerable(v4stashed), root.getEnumerable(v1stashed), new org.apache.calcite.linq4j.function.Predicate2() {
                    public boolean apply(Object[] left, Object[] right) {
                        return true;
                    }
                    public boolean apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , new org.apache.calcite.linq4j.function.Function2() {
                    public Object[] apply(Object[] left, Object[] right) {
                        return new Object[] {
                                left[0],
                                left[1],
                                left[2],
                                left[3],
                                left[4],
                                left[5],
                                right[0],
                                right[1],
                                right[2]};
                    }
                    public Object[] apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , org.apache.calcite.linq4j.JoinType.INNER));
        List list = _inputEnumerable.toList();
        final org.apache.calcite.linq4j.AbstractEnumerable _inputEnumerable0 = new org.apache.calcite.linq4j.AbstractEnumerable(){

            public org.apache.calcite.linq4j.Enumerator enumerator() {
                return new org.apache.calcite.linq4j.Enumerator(){

                    public final org.apache.calcite.linq4j.Enumerator inputEnumerator = Linq4j.asEnumerable(list).enumerator();
                    public void reset() {
                        inputEnumerator.reset();
                    }

                    public boolean moveNext() {
                        while (inputEnumerator.moveNext()) {
                            final Object[] current = (Object[]) inputEnumerator.current();
                            final Long input_value = (Long) current[0];
                            final Long input_value0 = (Long) current[6];
                            final Boolean binary_call_value = input_value == null || input_value0 == null ? (Boolean) null : Boolean.valueOf(input_value.longValue() == input_value0.longValue());
                            if (binary_call_value != null && org.apache.calcite.runtime.SqlFunctions.toBoolean(binary_call_value)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void close() {
                        inputEnumerator.close();
                    }

                    public Object current() {
                        final Object[] current = (Object[]) inputEnumerator.current();
                        final Object input_value = current[0];
                        final Object input_value0 = current[1];
                        final Object input_value1 = current[2];
                        final Object input_value2 = current[3];
                        final Object input_value3 = current[4];
                        final Object input_value4 = current[5];
                        final Object input_value5 = current[6];
                        final Object input_value6 = current[7];
                        final Object input_value7 = current[8];
                        return new Object[] {
                                input_value,
                                input_value0,
                                input_value1,
                                input_value2,
                                input_value3,
                                input_value4,
                                input_value5,
                                input_value6,
                                input_value7};
                    }

                };
            }

        };
        final org.apache.calcite.linq4j.AbstractEnumerable child = new org.apache.calcite.linq4j.AbstractEnumerable(){
            public org.apache.calcite.linq4j.Enumerator enumerator() {
                return new org.apache.calcite.linq4j.Enumerator(){
                    public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable0.enumerator();
                    public void reset() {
                        inputEnumerator.reset();
                    }

                    public boolean moveNext() {
                        return inputEnumerator.moveNext();
                    }

                    public void close() {
                        inputEnumerator.close();
                    }

                    public Object current() {
                        final Object[] current = (Object[]) inputEnumerator.current();
                        final Object input_value = current[0];
                        final Object input_value0 = current[1];
                        final Object input_value1 = current[2];
                        final Object input_value2 = current[3];
                        final Object input_value3 = current[4];
                        final Object input_value4 = current[5];
                        final Object input_value5 = current[6];
                        final Object input_value6 = current[7];
                        final Object input_value7 = current[8];
                        return new Object[] {
                                input_value,
                                input_value0,
                                input_value1,
                                input_value2,
                                input_value3,
                                input_value4,
                                input_value5,
                                input_value6,
                                input_value7};
                    }

                };
            }

        };
        return org.apache.calcite.linq4j.EnumerableDefaults.orderBy(child, new org.apache.calcite.linq4j.function.Function1() {
                    public Long apply(Object[] v) {
                        return (Long) v[0];
                    }
                    public Object apply(Object v) {
                        return apply(
                                (Object[]) v);
                    }
                }
                , (Comparator)org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false), 0, 2147483647);
    }


    public Class getElementType() {
        return java.lang.Object[].class;
    }


}
