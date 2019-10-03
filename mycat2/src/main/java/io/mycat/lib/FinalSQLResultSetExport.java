package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.lib.impl.FinalCacheLib;
import io.mycat.lib.impl.Response;

public class FinalSQLResultSetExport implements InstructionSet {

    public static Response responseFinalSQL(String key) {
        return FinalCacheLib.responseFinalCache(key);
    }

    public static void finalSQLFile(String fileName) {
        FinalCacheLib.finalCacheFile(fileName);
    }

    public static void initFinalSQLCacheFile(String cachePath) {
        FinalCacheLib.initFinalCacheFile(cachePath);
    }
}