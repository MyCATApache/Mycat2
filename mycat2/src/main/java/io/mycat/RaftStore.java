package io.mycat;

import java.util.Map;

public class RaftStore implements CoordinatorMetadataStorageManager.Store {
    @Override
    public void addChangedCallback(CoordinatorMetadataStorageManager.ChangedCallback changedCallback) {

    }

    @Override
    public void begin() {

    }

    @Override
    public String get(String schema) {
        return null;
    }

    @Override
    public void set(String schemas, String transformation) {

    }

    @Override
    public void set(String schemas, Map<String, String> transformation) {

    }

    @Override
    public Map<String, String> getMap(String schemas) {
        return null;
    }

    @Override
    public void commit() {

    }

    @Override
    public void close() {

    }
}
