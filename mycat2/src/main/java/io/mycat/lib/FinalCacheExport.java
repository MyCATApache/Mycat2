package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.lib.impl.FinalCacheLib;
import io.mycat.lib.impl.Response;

public class FinalCacheExport implements InstructionSet {

    public static Response responseFinalCache(String key) {
        return FinalCacheLib.responseFinalCache(key);
    }

    public static void finalCacheFile(String fileName) {
        FinalCacheLib.finalCacheFile(fileName);
    }

    public static void initFinalCacheFile(String cachePath) {
        FinalCacheLib.initFinalCacheFile(cachePath);
    }
}