package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.HexFunction;
import io.mycat.calcite.sqlfunction.stringfunction.UnhexFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.record.Record;
import org.apache.calcite.avatica.util.ByteString;

import java.util.Collections;
import java.util.List;

public class UnHexFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "unhex(string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static final class Func extends BinarySequenceFunction {

        private final Function arg;
        private boolean isNull;

        public Func(Function arg) {
            super();
            this.arg = arg;
        }

        @Override
        public List<Function> getArgs() {
            return Collections.singletonList(arg);
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public BinarySequence getBinary(Record rec) {
            CharSequence value = arg.getString(rec);
            isNull = arg.isNull(rec);
            if (isNull) return null;
            ByteString unhex = UnhexFunction.unhex(value.toString());
            isNull = unhex == null;
            if (isNull){
                return null;
            }
            return BinarySequence.of(unhex.getBytes());
        }
    }
}
