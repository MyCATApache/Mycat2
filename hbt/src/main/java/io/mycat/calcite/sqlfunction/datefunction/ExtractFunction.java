//package io.mycat.calcite.sqlfunction.datefunction;
//
//import org.apache.calcite.mycat.MycatSqlDefinedFunction;
//import org.apache.calcite.schema.ScalarFunction;
//import org.apache.calcite.schema.impl.ScalarFunctionImpl;
//import org.apache.calcite.sql.SqlFunction;
//
//import java.time.LocalDate;
//
//public class ExtractFunction extends SqlFunction {
//    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DayOfMonthFunction.class,
//            "extract");
//    public static DayOfMonthFunction INSTANCE = new DayOfMonthFunction();
//
//    public ExtractFunction() {
//        super("extract", scalarFunction);
//    }
//
//    public static Integer extract(String unit,) {
//        if (date == null){
//            return null;
//        }
//        return date.getDayOfMonth();
//    }
//}
