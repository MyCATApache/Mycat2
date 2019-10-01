package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.lib.impl.Response;
import io.mycat.lib.impl.TransforFileLib;

public class TransforFileExport implements InstructionSet {
    public static Response transferTo(String file) {
        return TransforFileLib.transferTo(file);
    }
}