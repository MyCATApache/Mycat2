//package io.mycat.lib;
//
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.lib.impl.CalciteLib;
//import io.mycat.lib.impl.Response;
//import io.mycat.pattern.InstructionSet;
//import org.apache.calcite.rel.RelNode;
//
//import java.util.function.Supplier;
//
//public class CalciteExport implements InstructionSet {
//
//
//    public final static  Response responseQueryCalcite(String sql) {
//        return CalciteLib.INSTANCE.responseQueryCalcite(sql);
//    }
//
//    public final static Supplier<MycatResultSetResponse[]> queryCalcite(String sql) {
//        return CalciteLib.INSTANCE.queryCalcite(sql);
//    }
//    public final static Supplier<MycatResultSetResponse[]> queryCalcite(RelNode rootRel){
//        return CalciteLib.INSTANCE.queryCalcite(rootRel);
//    }
//    public   final static   Response responseTest(){
//        return  CalciteLib.INSTANCE.responseTest();
//    }
//}