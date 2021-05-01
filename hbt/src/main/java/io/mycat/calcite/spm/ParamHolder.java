package io.mycat.calcite.spm;

import java.util.List;

public class ParamHolder {
    public final static ThreadLocal<List<Object>> CURRENT_THREAD_LOCAL = new ThreadLocal<>();
}
