package io.mycat.serializable;

public interface MaterializedRecordSetFactory {
    public static final  MaterializedRecordSetFactory DEFAULT_FACTORY = new MaterializedRecordSetFactoryImpl();
    OffHeapObjectList createFixedSizeRecordSet(int expectSize);
    OffHeapObjectList createRecordSet();
}
