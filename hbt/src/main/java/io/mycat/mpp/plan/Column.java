package io.mycat.mpp.plan;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Comparator;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
public class Column {
    final String name;
    final Class clazz;

    public static Column of(String name, Class clazz) {
        return new Column(name, clazz);
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", clazz=" + clazz +
                '}';
    }


    public Comparator<DataAccessor> createComparator(int field) {
        if (Comparable.class.isAssignableFrom(clazz)) {
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