package io.mycat;

public interface ConfigChangeListener {
    void configChanged(String description);
}