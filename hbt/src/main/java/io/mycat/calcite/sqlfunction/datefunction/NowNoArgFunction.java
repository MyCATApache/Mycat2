package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

import java.time.LocalDateTime;

public class NowNoArgFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(NowNoArgFunction.class,
            "now");
    public static NowNoArgFunction INSTANCE = new NowNoArgFunction();

    public NowNoArgFunction() {
        super("NOW",
                scalarFunction
        );
    }

    public static LocalDateTime now() {
        return NowFunction.now(null);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparse(writer, call, leftPrec, rightPrec);
    }
}
