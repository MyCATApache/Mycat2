//package io.mycat.hbt3;
//
//import com.alibaba.fastsql.sql.ast.SQLExpr;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.sql.type.SqlTypeName;
//
//import java.util.List;
//import java.util.function.Function;
//
//public class PartitionMethodFactory {
//    static Function<Object, Integer> getByName(String methodName, List<SQLExpr> clazz, RelDataType p, int dbPartitionNum) {
//        if (Number.class.isAssignableFrom(clazz)) {
//            return o -> {
//                Number o1 = (Number) o;
//                return (int) o1.longValue() % p;
//            };
//        }
//        if (String.class.isAssignableFrom(clazz)) {
//            return o -> {
//                String o1 = (String) o;
//                return o1.hashCode();
//            };
//        }
//        throw new UnsupportedOperationException();
//
//    }
//
//    public static Function<Object, Integer> getByName(String methodName, SqlTypeName sqlTypeName) {
//        return o -> {
//            String o1 = (String) o;
//            return o1.hashCode();
//        };
//    }
//}