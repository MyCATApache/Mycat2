package io.mycat.newquery;

import io.mycat.beans.mycat.MycatRowMetaData;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
}
