package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.lib.impl.CalciteLib;
import io.mycat.lib.impl.Response;

import java.util.function.Supplier;

public class CalciteExport implements InstructionSet {


    public final static  Response responseQueryCalcite(String sql) {
        return CalciteLib.INSTANCE.responseQueryCalcite(sql);
    }

    public final static Supplier<MycatResultSetResponse[]> queryCalcite(String sql) {
        return CalciteLib.INSTANCE.queryCalcite(sql);
    }
}