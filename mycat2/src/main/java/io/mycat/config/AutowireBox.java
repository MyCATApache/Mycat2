package io.mycat.config;

import java.util.Set;

public abstract class AutowireBox {
    abstract public Set<Class> dependents();

}