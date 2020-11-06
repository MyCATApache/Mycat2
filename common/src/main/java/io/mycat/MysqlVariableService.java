package io.mycat;

import java.util.Objects;

public interface MysqlVariableService {

    default Object getVariable(String name) {
        String s = Objects.requireNonNull(name);
        if (s.startsWith("@@")) {
            return getGlobalVariable(name);
        }
        return getSessionVariable(name);
    }

    Object getGlobalVariable(String name);

    Object getSessionVariable(String name);

    int getStoreNodeNum();
}
