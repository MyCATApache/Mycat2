package io.mycat.hbt3;

public interface PartInfo {

    int size();

    String toString();

    Part getPart(int index);

    Part[] toPartArray();
}