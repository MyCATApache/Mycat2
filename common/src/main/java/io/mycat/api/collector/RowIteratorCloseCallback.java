package io.mycat.api.collector;

public interface RowIteratorCloseCallback {
    void onClose(long rowCount);
}