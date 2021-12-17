package io.ordinate.engine.function.constant;

import io.ordinate.engine.record.Record;
import io.ordinate.engine.schema.InnerType;

import java.time.Period;

public class PeriodConstant  implements ConstantFunction {
   final Period period;

    public PeriodConstant(Period period) {
        this.period = period;
    }

    public static PeriodConstant of(Period period){
        return new PeriodConstant(period);
    }

    @Override
    public InnerType getType() {
        return InnerType.SYMBOL_TYPE;
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return period.toString();
    }

    @Override
    public Object getAsObject(Record rec) {
        return period;
    }

    @Override
    public boolean isNull(Record rec) {
        return false;
    }
}
