//package io.mycat.lib;
//
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.lib.impl.Response;
//import io.mycat.lib.impl.TransforFileLib;
//import io.mycat.pattern.InstructionSet;
//
//import java.io.IOException;
///**
// * @author chen junwen
// */
//public class TransforFileExport implements InstructionSet {
//    public static Response transferFileTo(String file) {
//        return TransforFileLib.transferFileTo(file);
//    }
//
//    public static void saveToFile(String filePath, boolean eof, MycatResultSetResponse<byte[]> resultSetResponse) throws IOException {
//        TransforFileLib.saveToFile(filePath, eof, resultSetResponse);
//    }
//}