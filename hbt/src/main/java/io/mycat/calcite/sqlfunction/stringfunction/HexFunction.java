package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.fastsql.util.HexBin;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;


public class HexFunction extends MycatSqlDefinedFunction {


    public static final HexFunction INSTANCE = new HexFunction();

    public HexFunction() {
        super("hex",
                ReturnTypes.VARCHAR_2000, InferTypes.FIRST_KNOWN, OperandTypes.ANY, null, SqlFunctionCategory.STRING);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method method = Types.lookupMethod(HexFunction.class, "hex", Object.class);
        return Expressions.call(method, translator.translateList(call.getOperands(), nullAs));
    }

    public static String hex(Object param0Value) {
        if (param0Value instanceof String) {
            byte[] bytes = ((String) param0Value).getBytes();
            String result = HexBin.encode(bytes);
            return result;
        }

        if (param0Value instanceof Number) {
            long value = ((Number) param0Value).longValue();
            String result = Long.toHexString(value).toUpperCase();
            return result;
        }
        if (param0Value instanceof ByteString) {
            String result = HexBin.encode(((ByteString) param0Value).getBytes());
            return result;
        }
        throw new UnsupportedOperationException();
    }
}