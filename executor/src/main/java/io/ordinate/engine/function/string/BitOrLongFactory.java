package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.BitOrFunction;
import io.mycat.calcite.sqlfunction.stringfunction.MakeSetFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.LongFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;
import java.util.stream.Collectors;

public class BitOrLongFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "|(long,long)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends LongFunction {

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
        public long getLong(Record rec) {
            Function one = this.args.get(0);
            Function two = this.args.get(1);
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return 0;
            Long bitOr = BitOrFunction.bitOr(one.getLong(rec), two.getLong(rec));
            isNull = bitOr == null;
            if (isNull) return 0;
            return bitOr;
        }

    }
}
