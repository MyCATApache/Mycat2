package io.mycat.calcite.sqlfunction.stringfunction;


import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class ConvertFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ConvertFunction.class,
            "convert");
    public static final ConvertFunction INSTANCE = new ConvertFunction();

    public ConvertFunction() {
        super("convert", scalarFunction);
    }

    @SneakyThrows
    public static String convert(String expr, String charset) {
        if (expr == null) {
            return expr;
        }
        return new String(expr.getBytes(charset));
    }

    @Override
    public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("USING");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }
}