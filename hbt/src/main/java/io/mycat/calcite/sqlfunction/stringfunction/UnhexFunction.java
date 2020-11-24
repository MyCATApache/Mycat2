package io.mycat.calcite.sqlfunction.stringfunction;


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
import org.apache.calcite.sql.type.*;

import java.lang.reflect.Method;

public class UnhexFunction extends MycatSqlDefinedFunction {

    public static final UnhexFunction INSTANCE = new UnhexFunction();

    public UnhexFunction() {
        super("unhex",
                ReturnTypes.explicit(SqlTypeName.BINARY), InferTypes.FIRST_KNOWN, OperandTypes.STRING, null, SqlFunctionCategory.STRING);

    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method method = Types.lookupMethod(UnhexFunction.class,
                "unhex", String.class);
        return Expressions.call(method,translator.translateList(call.getOperands(),nullAs));
    }

    public static ByteString unhex(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return ByteString.EMPTY;
        }
        try {
            return new ByteString(ByteString.parse(str, 16));
        }catch (Throwable ignored){
            return null;
        }
    }
}