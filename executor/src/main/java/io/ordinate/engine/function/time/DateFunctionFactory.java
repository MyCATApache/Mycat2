package io.ordinate.engine.function.time;

import io.ordinate.engine.function.cast.CastStringToDateFunctionFactory;

public class DateFunctionFactory extends CastStringToDateFunctionFactory {
    @Override
    public String getSignature() {
        return "date(string):date";
    }

}
