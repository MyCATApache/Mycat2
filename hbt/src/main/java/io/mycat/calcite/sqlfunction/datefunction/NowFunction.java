package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class NowFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(NowFunction.class,
            "now");
    public static NowFunction INSTANCE = new NowFunction();

    public NowFunction() {
        super("NOW",
                scalarFunction
        );
    }

    public static LocalDateTime now(@Parameter(name = "precision") Integer precision) {
        if (precision == null) {
            return LocalDateTime.now(ZoneId.systemDefault()).now().withNano(0);
        }
        LocalDateTime now = LocalDateTime.now();
        int nano = now.getNano();
        //999,999,999

        int i1 =(int) Math.pow(10,(9 - precision));
        nano= nano/i1*i1;
        return now.withNano(nano);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparse(writer, call, leftPrec, rightPrec);
    }
}
