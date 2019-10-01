package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.lib.impl.Response;
import io.mycat.lib.impl.TransforFileLib;

import java.io.IOException;

public class TransforFileExport implements InstructionSet {
    public static Response transferTo(String file) {
        return TransforFileLib.transferTo(file);
    }

    public static void saveToFile(String filePath, boolean eof, MycatResultSetResponse<byte[]> resultSetResponse) throws IOException {
        TransforFileLib.saveToFile(filePath, eof, resultSetResponse);
    }
}