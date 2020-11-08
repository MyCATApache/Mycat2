package org.apache.calcite.mycat;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.jdbc.JavaTypeFactoryImpl.toSql;

public abstract class MycatSqlDefinedFunction extends SqlFunction implements CallImplementor {

    final ScalarFunction function;

    public MycatSqlDefinedFunction(String name,
                                   SqlReturnTypeInference returnTypeInference,
                                   SqlOperandTypeInference operandTypeInference,
                                   SqlOperandTypeChecker operandTypeChecker,
                                   ScalarFunction function,
                                   SqlFunctionCategory category) {
        super(name,
                new SqlIdentifier(name, SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category);
        this.function = function;
    }

    public MycatSqlDefinedFunction(String name,
                                   ScalarFunction function,
                                   SqlFunctionCategory category) {
        this(name, new GetOperandTypes(function).invoke(), category);
    }


    public MycatSqlDefinedFunction(String name,
                                   GetOperandTypes getOperandTypes,
                                   SqlFunctionCategory category) {
        this(name, getOperandTypes.getSqlReturnTypeInference(),
                getOperandTypes.getOperandTypeInference(),
                getOperandTypes.getOperandMetadata(), getOperandTypes.function, category);
    }

    public String getName() {
        return this.getSqlIdentifier().getSimple();
    }

    public static class GetOperandTypes {
        private ScalarFunction function;
        private SqlOperandTypeInference operandTypeInference;
        private SqlOperandMetadata operandMetadata;

        public GetOperandTypes(ScalarFunction function) {
            this.function = Objects.requireNonNull(function);
        }

        public SqlOperandTypeInference getOperandTypeInference() {
            return operandTypeInference;
        }

        public SqlOperandTypeChecker getOperandMetadata() {
            return operandMetadata;
        }

        public GetOperandTypes invoke() {
            final java.util.function.Function<RelDataTypeFactory, List<RelDataType>> argTypesFactory =
                    typeFactory -> function.getParameters()
                            .stream()
                            .map(o -> o.getType(typeFactory))
                            .collect(Util.toImmutableList());
            final java.util.function.Function<RelDataTypeFactory, List<SqlTypeFamily>> typeFamiliesFactory =
                    typeFactory -> argTypesFactory.apply(typeFactory)
                            .stream()
                            .map(type ->
                                    Util.first(type.getSqlTypeName().getFamily(),
                                            SqlTypeFamily.ANY))
                            .collect(Util.toImmutableList());
            final java.util.function.Function<RelDataTypeFactory, List<RelDataType>> paramTypesFactory =
                    typeFactory ->
                            argTypesFactory.apply(typeFactory)
                                    .stream()
                                    .map(type -> toSql(typeFactory, type))
                                    .collect(Util.toImmutableList());

            // Use a short-lived type factory to populate "typeFamilies" and "argTypes".
            // SqlOperandMetadata.paramTypes will use the real type factory, during
            // validation.
            final RelDataTypeFactory dummyTypeFactory = new JavaTypeFactoryImpl();
            final List<RelDataType> argTypes = argTypesFactory.apply(dummyTypeFactory);
            final List<SqlTypeFamily> typeFamilies =
                    typeFamiliesFactory.apply(dummyTypeFactory);

            operandTypeInference = InferTypes.explicit(argTypes);

            operandMetadata = OperandTypes.operandMetadata(typeFamilies, paramTypesFactory,
                    i -> function.getParameters().get(i).getName(),
                    i -> function.getParameters().get(i).isOptional());
            return this;
        }

        public SqlReturnTypeInference getSqlReturnTypeInference() {
            return CalciteCatalogReader.infer(function);
        }
    }

    public ScalarFunction getFunction() {
        return function;
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        return ((ImplementableFunction) function).getImplementor().implement(translator, call, nullAs);
    }
}
