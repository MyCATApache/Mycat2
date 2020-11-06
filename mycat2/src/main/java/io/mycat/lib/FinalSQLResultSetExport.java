//package io.mycat.lib;
//
//import io.mycat.lib.impl.FinalCacheLib;
//import io.mycat.lib.impl.Response;
//import io.mycat.pattern.InstructionSet;
//
///**
// * @author chen junwen
// */
//public class FinalSQLResultSetExport implements InstructionSet {
//
//    public static Response responseFinalSQL(String key) {
//        return FinalCacheLib.responseFinalCache(key);
//    }
//
//    public static void finalSQLFile(String fileName) {
//        FinalCacheLib.finalCacheFile(fileName);
//    }
//
//    public static void initFinalSQLCacheFile(String cachePath) {
//        FinalCacheLib.initFinalCacheFile(cachePath);
//    }
//}