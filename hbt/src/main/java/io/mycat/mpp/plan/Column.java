package io.mycat.mpp.plan;


import io.mycat.mpp.runtime.Type;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Comparator;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
public class Column {
    final String name;
    final Type type;

    public static Column of(String name, Type clazz) {
        return new Column(name, clazz);
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }


    public Comparator<DataAccessor> createComparator(int field) {
        if (Comparable.class.isAssignableFrom(type.getJavaClass())) {
            return (o1, o2) -> {
                Comparable l = (Comparable)o1.get(field);
                if (l == null)return -1;
                Comparable r =(Comparable) o2.get(field);
                if (r == null)return 1;
                return l.compareTo(r);
            };
        }else {
            Comparator tComparator = Comparator.naturalOrder();
            return tComparator;
        }
    }
}