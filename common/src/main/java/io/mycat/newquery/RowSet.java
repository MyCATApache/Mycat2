package io.mycat.newquery;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class RowSet implements Iterable<Object[]>{
    private MycatRowMetaData mycatRowMetaData;
    private List<Object[]> objects;

    public RowSet(MycatRowMetaData mycatRowMetaData, List<Object[]> objects) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.objects = objects;
    }

    public int affectRows() {
        return objects.size();
    }

    public int size() {
        return objects.size();
    }

    @NotNull
    @Override
    public Iterator<Object[]> iterator() {
        return objects.iterator();
    }

   public RowBaseIterator toRowBaseIterator(){
        return new ResultSetBuilder.DefObjectRowIteratorImpl(mycatRowMetaData,objects.iterator());
   }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner("\n");
        for (Object[] i : objects) {
            String s;

            List<String> rowString = new ArrayList<>();
            for (Object o : i) {
                if (o instanceof byte[]) {
                    o = new String((byte[])o);
                }
                rowString.add(Objects.toString(o));
            }
            s = rowString.toString();


            joiner.add(s);
        }
        return "RowSet{" +
                "mycatRowMetaData=" + mycatRowMetaData +
                ", objects=" + joiner.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowSet objects1 = (RowSet) o;
        return Objects.equals(mycatRowMetaData, objects1.mycatRowMetaData) && Objects.equals(objects, objects1.objects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mycatRowMetaData, objects);
    }
}
