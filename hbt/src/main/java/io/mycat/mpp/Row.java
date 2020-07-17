package io.mycat.mpp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.calcite.linq4j.function.Function2;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Row {
    public Object[] values;

    @NotNull
    public static Function2<Row, Row, Row> composeJoinRow(int leftFieldCount, int rightFieldCount) {
        return (v0, v1) -> {
            if (v0 == null) {
                v0 = Row.create(leftFieldCount);
            }
            if (v1 == null) {
                v1 = Row.create(rightFieldCount);
            }
            return v0.compose(v1);
        };
    }

    public Row compose(Row right) {
        Row row = new Row();
        int newLength = this.values.length + right.values.length;
        row.values = new Object[newLength];
        System.arraycopy(this.values, 0, row.values, 0, this.values.length);
        System.arraycopy(right.values, 0, row.values, this.values.length, right.values.length);
        return row;
    }

    public Object getObject(int i) {
        return values[i];
    }

    public static Row create(int size) {
        Row row = new Row();
        row.values = new Object[size];
        return row;
    }

    public static Row of(Object[] objects) {
        Row row = new Row();
        row.values = objects;
        return row;
    }

    public void set(int i, Object object) {
        values[i] = object;
    }

    public int size() {
        return values.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Row row = (Row) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        int length = values.length;
        for (int i = 0; i < length; i++) {
          if (compare(values[i], row.values[i]) != 0) {
              return false;
          }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    static int compare(Object a, Object b) {
        if (a == null) {
            if (b == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (b == null) {
            return -1;
        }
        if (a instanceof String && b instanceof String) {
            return -((String) b).compareTo((String) a);
        }
        if (a instanceof Integer) {
            if (b instanceof Integer) {
                return (Integer) a - (Integer) b;
            }
            if (b instanceof Long) {
                long delta = (Integer) a - (Long) b;
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
        }
        if (a instanceof Long) {
            if (b instanceof Long) {
                long delta = (Long) a - (Long) b;
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
            if (b instanceof java.util.Date) {
                long delta = ((Long) a) - ((java.util.Date) b).getTime();
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
        }
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        if (a instanceof java.util.Date) {
            if (b instanceof java.util.Date) {
                long delta = ((java.util.Date) a).getTime() - ((java.util.Date) b).getTime();
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
            if (b instanceof java.lang.Long) {
                long delta = ((java.util.Date) a).getTime() - ((Long) b);
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b);
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return Arrays.equals((byte[]) a, (byte[]) b) ? 0 : ((byte[]) a).length - ((byte[]) b).length;
        }
        throw new IllegalArgumentException("unsupported comparable objects " + a.getClass() + " ('" + a + "') vs " + b.getClass() + " ('" + b + "')");
    }

}