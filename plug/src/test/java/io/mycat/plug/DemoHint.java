package io.mycat.plug;

import io.mycat.Hint;

import java.util.Map;

public class DemoHint implements Hint {
    String name;

    public DemoHint(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void accept(String buffer, Map<String, Object> t) {

    }
}