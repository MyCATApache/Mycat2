package io.mycat.sqlhandler.config;

import java.util.*;

public abstract class AbstractStorageManagerImpl implements StorageManager {
    protected final Set<Class> registerClassSet = Collections.newSetFromMap(new IdentityHashMap<>());

    @Override
    public void register(Class aClass) {
        registerClassSet.add(aClass);
    }

    @Override
    public Collection<Class> registerClasses() {
        return new HashSet<>(registerClassSet);
    }


}
