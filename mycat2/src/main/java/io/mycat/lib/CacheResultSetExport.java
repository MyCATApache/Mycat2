package io.mycat.lib;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.lib.impl.CacheLib;
import io.mycat.pattern.InstructionSet;

import java.util.function.Supplier;

/**
 * @author chen junwen
 */
public class CacheResultSetExport implements InstructionSet {

    public static MycatResultSetResponse cacheResponse(String key, Supplier<MycatResultSetResponse> supplier) {
        return CacheLib.cacheResponse(key,supplier);
    }

    public static void removeCache(String key) {
         CacheLib.removeCache(key);
    }
}