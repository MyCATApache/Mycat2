package io.mycat.hbt3;

import java.util.stream.IntStream;


public class RangePartInfo implements PartInfo {

    String schema;
    String table;

    int startIndex;
    int endIndex;

    public RangePartInfo(String schema, String table, int startIndex, int endIndex) {
        this.schema = schema;
        this.table = table;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    public int size() {
        return endIndex-startIndex;
    }

    @Override
    public Part getPart(int index) {
        return new PartImpl(schema+"."+table+"["+index+"]");
    }

    @Override
    public Part[] toPartArray() {
        return IntStream.rangeClosed(startIndex,endIndex).mapToObj(i->getPart(i)).toArray(i->new Part[i]);
    }

    @Override
    public String toString() {
        return "[" + schema+"."+table+"["+startIndex+"-"+endIndex+"]" +"]";
    }
}