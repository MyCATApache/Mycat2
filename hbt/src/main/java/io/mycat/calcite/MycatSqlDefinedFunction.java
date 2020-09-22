package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.util.List;
import java.util.stream.Collectors;

public abstract class MycatSqlDefinedFunction extends SqlUserDefinedFunction {
//
//    public static final MycatCalciteSupport.MycatTypeSystem TypeSystem = MycatCalciteSupport.INSTANCE.TypeSystem;
//    public static final JavaTypeFactoryImpl TypeFactory =MycatCalciteSupport.INSTANCE.TypeFactory;
        public MycatSqlDefinedFunction(SqlIdentifier opName, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes, Function function) {
        super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, function);
    }

    public MycatSqlDefinedFunction(SqlIdentifier opName, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes, Function function, SqlFunctionCategory category) {
        super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, function, category);
    }

    public static List<RelDataType> getRelDataType(ScalarFunction scalarFunction) {
        MycatCalciteSupport instance = MycatCalciteSupport.INSTANCE;
        return scalarFunction.getParameters().stream().map(i -> i.getType(MycatCalciteSupport.INSTANCE.TypeFactory)).collect(Collectors.toList());
    }

    public String getName(){
          return this.getSqlIdentifier().getSimple();
    }
}