package java.time.temporal;

import java.time.LocalTime;

public class TemporalImpl implements Temporal {
    long days;
    LocalTime localTime;
    @Override
    public boolean isSupported(TemporalUnit unit) {
        return false;
    }

    @Override
    public Temporal with(TemporalField field, long newValue) {
        return null;
    }

    @Override
    public Temporal plus(long amountToAdd, TemporalUnit unit) {
        return null;
    }

    @Override
    public long until(Temporal endExclusive, TemporalUnit unit) {
        return 0;
    }

    @Override
    public boolean isSupported(TemporalField field) {
        return false;
    }

    @Override
    public long getLong(TemporalField field) {
        return 0;
    }
}
