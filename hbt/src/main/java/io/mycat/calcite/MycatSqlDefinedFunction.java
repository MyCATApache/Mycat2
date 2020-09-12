package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.util.List;

public class MycatSqlDefinedFunction extends SqlUserDefinedFunction {
    public MycatSqlDefinedFunction(SqlIdentifier opName, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes, Function function) {
        super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, function);
    }

    public MycatSqlDefinedFunction(SqlIdentifier opName, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes, Function function, SqlFunctionCategory category) {
        super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, function, category);
    }
}