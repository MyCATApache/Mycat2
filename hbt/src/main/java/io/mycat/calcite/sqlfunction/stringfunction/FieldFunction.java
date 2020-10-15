package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class FieldFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FieldFunction.class,
            "field");
    public static final FieldFunction INSTANCE = new FieldFunction();

    public FieldFunction() {
        super("field", scalarFunction);
    }

    @SneakyThrows
    public static Integer field(String... args) {
        if (args.length < 2) {
       throw new IllegalArgumentException("1582");
        }
        String pat = args[0];
        if (pat==null){
            return null;
        }
        for (int i = 1; i <args.length ; i++) {
            if (pat.equalsIgnoreCase(args[i])){
                return i;
            }
        }
        return 0;
    }

    @SneakyThrows
    public static Integer field(Number... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("1582");
        }
        Number pat = args[0];
        if (pat==null){
            return null;
        }
        for (int i = 1; i <args.length ; i++) {
            if (pat.equals(args[i])){
                return i;
            }
        }
        return 0;
    }
}