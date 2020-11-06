//package java.time.temporal;
//
//public class MycatLocalTime implements Temporal {
//    long second;
//    long nanos;
//    @Override
//    public boolean isSupported(TemporalUnit unit) {
//        return false;
//    }
//
//    @Override
//    public Temporal with(TemporalField field0, long newValue) {
//        if ( field0 instanceof ChronoField){
//            ChronoField   field = (ChronoField)field0;
//            switch (field) {
//                case NANO_OF_SECOND:
//                    nanos = (int)newValue;
//                    break;
//                case NANO_OF_DAY:
//                    nanos = (int)newValue;
//                    break;
//                case MICRO_OF_SECOND:
//                    nanos = (int)newValue;
//                    break;
//                case MICRO_OF_DAY:
//                    break;
//                case MILLI_OF_SECOND:
//                    break;
//                case MILLI_OF_DAY:
//                    break;
//                case SECOND_OF_MINUTE:
//                    break;
//                case SECOND_OF_DAY:
//                    break;
//                case MINUTE_OF_HOUR:
//                    break;
//                case MINUTE_OF_DAY:
//                    break;
//                case HOUR_OF_AMPM:
//                    break;
//                case CLOCK_HOUR_OF_AMPM:
//                    break;
//                case HOUR_OF_DAY:
//                    break;
//                case CLOCK_HOUR_OF_DAY:
//                    break;
//                case AMPM_OF_DAY:
//                    break;
//                case DAY_OF_WEEK:
//                    break;
//                case ALIGNED_DAY_OF_WEEK_IN_MONTH:
//                    break;
//                case ALIGNED_DAY_OF_WEEK_IN_YEAR:
//                    break;
//                case DAY_OF_MONTH:
//                    break;
//                case DAY_OF_YEAR:
//                    break;
//                case EPOCH_DAY:
//                    break;
//                case ALIGNED_WEEK_OF_MONTH:
//                    break;
//                case ALIGNED_WEEK_OF_YEAR:
//                    break;
//                case MONTH_OF_YEAR:
//                    break;
//                case PROLEPTIC_MONTH:
//                    break;
//                case YEAR_OF_ERA:
//                    break;
//                case YEAR:
//                    break;
//                case ERA:
//                    break;
//                case INSTANT_SECONDS:
//                    break;
//                case OFFSET_SECONDS:
//                    break;
//            }
//        }
//        return null;
//    }
//
//    @Override
//    public Temporal plus(long amountToAdd, TemporalUnit unit) {
//        return null;
//    }
//
//    @Override
//    public long until(Temporal endExclusive, TemporalUnit unit) {
//        return 0;
//    }
//
//    @Override
//    public boolean isSupported(TemporalField field) {
//        return false;
//    }
//
//    @Override
//    public long getLong(TemporalField field) {
//        return 0;
//    }
//}
