package io.mycat;

import io.mycat.util.Dumper;

public interface Dumpable {
    public Dumper snapshot();
}