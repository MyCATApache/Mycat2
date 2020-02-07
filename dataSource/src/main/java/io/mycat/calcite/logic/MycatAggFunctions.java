package io.mycat.calcite.logic;

import com.google.common.base.Preconditions;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.sql.type.ReturnTypes.*;

public class MycatAggFunctions {

    public static MyCountAggFunction AVG = new MyCountAggFunction(SqlKind.AVG);

    /**
     * User-defined aggregate function.
     */
    public static class MyCountAggFunction extends SqlAggFunction {
        //~ Constructors -----------------------------------------------------------

        /**
         * Creates a SqlAvgAggFunction.
         */
        public MyCountAggFunction(SqlKind kind) {
            this(kind.name(), kind);
        }

        MyCountAggFunction(String name, SqlKind kind) {
            super(name,
                    null,
                    kind,
                    ReturnTypes.AVG_AGG_FUNCTION,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC,
                    false,
                    false,
                    Optionality.FORBIDDEN);
            Preconditions.checkArgument(SqlKind.AVG_AGG_FUNCTIONS.contains(kind),
                    "unsupported sql kind");
        }

        @Deprecated // to be removed before 2.0
        public MyCountAggFunction(
                RelDataType type,
                SqlAvgAggFunction.Subtype subtype) {
            this(SqlKind.valueOf(subtype.name()));
        }

        //~ Methods ----------------------------------------------------------------

        /**
         * Returns the specific function, e.g. AVG or STDDEV_POP.
         *
         * @return Subtype
         */
        @Deprecated // to be removed before 2.0
        public SqlAvgAggFunction.Subtype getSubtype() {
            return SqlAvgAggFunction.Subtype.valueOf(kind.name());
        }

        /**
         * Sub-type of aggregate function.
         */
        @Deprecated // to be removed before 2.0
        public enum Subtype {
            AVG,
            STDDEV_POP,
            STDDEV_SAMP,
            VAR_POP,
            VAR_SAMP
        }
    }

    /**
     * "MYAGG" user-defined aggregate function. This agg function accept two numeric arguments
     * in order to reproduce the throws of CALCITE-2744.
     */
    public static class MyAvgAggFunction extends SqlAggFunction {
        static final List<RelDataType> apply = Collections.singletonList(RelDataTypeFactoryImpl.JavaType.proto(SqlTypeName.DOUBLE, true).apply(new JavaTypeFactoryImpl()));

        public MyAvgAggFunction() {
            super(SqlKind.AVG.name(), null, SqlKind.AVG, opBinding -> {
                        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                        final RelDataType relDataType = typeFactory.createJavaType(Double.class);
                        if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
                            return typeFactory.createTypeWithNullability(relDataType, true);
                        } else {
                            return relDataType;
                        }
                    },
                    null, OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC, false, false, Optionality.FORBIDDEN);
        }

        @Override
        public List<RelDataType> getParamTypes() {
            return apply;
            // return Arrays.asList(apply);
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }
    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public static final SqlBinaryOperator DIVIDE
            ;

    public static final SqlBinaryOperator DIVIDE2 ;

    static {
//        DIVIDE = new SqlBinaryOperator(
//                "/",
//                SqlKind.DIVIDE,
//                60,
//                true,
//                chain(DECIMAL_QUOTIENT_NULLABLE, ARG0_INTERVAL_NULLABLE,
//                        opBinding -> opBinding.getTypeFactory().createJavaType(Double.class)),
//                InferTypes.RETURN_TYPE,
//                OperandTypes.DIVISION_OPERATOR);
        DIVIDE = new SqlBinaryOperator(
                "/",
                SqlKind.DIVIDE,
                60,
                true,
                chain(DECIMAL_QUOTIENT_NULLABLE, ARG0_INTERVAL_NULLABLE,
                        opBinding -> opBinding.getTypeFactory().createJavaType(Double.class)),
                InferTypes.FIRST_KNOWN,
                OperandTypes.DIVISION_OPERATOR);
        List<RelDataType> relDataTypes = Arrays.asList(
                RelDataTypeFactoryImpl.JavaType.proto(SqlTypeName.INTEGER, true).apply(new JavaTypeFactoryImpl()),
                RelDataTypeFactoryImpl.JavaType.proto(SqlTypeName.INTEGER, true).apply(new JavaTypeFactoryImpl()));
        DIVIDE2 = new SqlBinaryOperator(
                "/",
                SqlKind.DIVIDE,
                60,
                true,
                        opBinding -> opBinding.getTypeFactory().createJavaType(Double.class),
                InferTypes.explicit(relDataTypes),
                OperandTypes.DIVISION_OPERATOR);
    }
}