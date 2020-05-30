package io.mycat.mpp;

import io.mycat.mpp.runtime.Type;

public class DataContext {
    public static DataContext DEFAULT = new DataContext();
    public Type guessVariantRefExpr(String name) {
        return null;
    }
    public Object getVariantRefExpr(String name) {
        return null;
    }
}