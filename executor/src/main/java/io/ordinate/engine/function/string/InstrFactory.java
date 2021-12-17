package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.InsertFunction;
import io.mycat.calcite.sqlfunction.stringfunction.InstrFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class InstrFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "instr(string,string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new InstrFactory.Func(args);
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
        public int getInt(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return 0;
            Integer instr = InstrFunction.instr(one.getString(rec).toString(), two.getString(rec).toString());
            isNull = instr == null;
            if (isNull) return 0;
            return instr;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}
