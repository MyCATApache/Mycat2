package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.ConvFunction;
import io.mycat.calcite.sqlfunction.stringfunction.InsertFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class InsertFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "INSERT(string,int,int,string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new InsertFactory.Func(args);
    }

    private static final class Func extends StringFunction {

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
        public CharSequence getString(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);
            Function three = args.get(2);
            Function four = args.get(3);
            isNull = one.isNull(rec) || two.isNull(rec) || three.isNull(rec) || four.isNull(rec);
            if (isNull) return null;
            String insert = InsertFunction.insert(one.getString(rec).toString(), two.getInt(rec), three.getInt(rec), four.getString(rec).toString());
            isNull = insert == null;
            if (isNull) return null;
            return insert;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}
