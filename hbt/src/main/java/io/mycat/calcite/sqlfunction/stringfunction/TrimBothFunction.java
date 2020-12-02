package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class TrimBothFunction extends MycatSqlDefinedFunction {

    public static final TrimBothFunction INSTANCE = new TrimBothFunction();

    public TrimBothFunction() {
        super("trim_both",
                ReturnTypes.VARCHAR_2000, InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, null, SqlFunctionCategory.STRING);

    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        if(call.getOperands().size()==2){
            return Expressions.call(Types.lookupMethod(TrimBothFunction.class,
                    "trim_both", String.class,String.class),
                    translator.translateList(call.getOperands(), nullAs));
        }
        return Expressions.call(Types.lookupMethod(TrimBothFunction.class,
                "trim", String.class),
                translator.translateList(call.getOperands(), nullAs));
    }
    public static String trim(String text) {
        if (text==null){
            return null;
        }
        return text.trim();
    }
    public static String trim_both(String needRemove, String needTrim) {
        if (needRemove == null || needTrim == null) {
            return null;
        }
        return StringUtils.strip(needTrim,needRemove);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.print("trim(both,");
        List<SqlNode> operandList = call.getOperandList();
        operandList.get(0).unparse(writer, 0, 0);
        writer.print(" from ");
        operandList.get(1).unparse(writer, 0, 0);
        writer.print(")");
    }
}