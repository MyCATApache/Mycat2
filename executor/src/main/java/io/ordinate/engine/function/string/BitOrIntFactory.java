package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.BitOrFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;
import org.apache.spark.sql.sources.In;

import java.util.List;

public class BitOrIntFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "|(int,int)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends IntFunction {

        private final List<Function> args;
        private boolean isNull;

        public Func(List<Function> args) {
            super();
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public int getInt(Record rec) {
            Function one = this.args.get(0);
            Function two = this.args.get(1);
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return 0;
            Integer bitOr = BitOrFunction.bitOr(one.getInt(rec), two.getInt(rec));
            isNull = bitOr == null;
            if (isNull) return 0;
            return bitOr;
        }

    }
}
