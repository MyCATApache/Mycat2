package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.*;

public class MycatDateFunction extends MycatSqlDefinedFunction {
//    public MycatDateFunction(String name,
//                             SqlReturnTypeInference returnTypeInference,
//                             SqlOperandTypeInference operandTypeInference,
//                             SqlOperandTypeChecker operandTypeChecker,
//                             ScalarFunction function) {
//        super(name, returnTypeInference, operandTypeInference, operandTypeChecker,function, SqlFunctionCategory.TIMEDATE);
//    }
//
//    public MycatDateFunction(String name, ScalarFunction function) {
//        this(name,new GetOperandTypes(function).invoke().getSqlReturnTypeInference(), InferTypes.VARCHAR_1024, OperandTypes .VARIADIC,function);
//    }
public MycatDateFunction(String name, ScalarFunction function) {
    super(name, function, SqlFunctionCategory.TIMEDATE);
}

}
