package io.mycat.router.range;

import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


@Getter
public abstract class Enumerator<T,U> {
    protected final int size;
    protected final T start;
    protected final  T end;
    protected  final  U unit;
    protected final   boolean cycle;
    protected final  int enumSize;

    public Enumerator(int size, T start, T end, U unit, boolean cycle, int enumSize) {
        this.size = size;
        this.start = start;
        this.end = end;
        this.unit = unit;
        this.cycle = cycle;
        this.enumSize = enumSize;
    }

    abstract public Optional<Iterable<T>> rangeClosed(T left, T right);

     public Optional<List<T>> rangeClosedAsList(T left, T right){
        return rangeClosed(left,right).map(i-> StreamSupport.stream(i.spliterator(),false).collect(Collectors.toList()));
    }
//
//    public Optional<Iterable<T>> range(T left, T right) {
//        if (left instanceof Integer && right instanceof Integer) {
//            return (Optional) rangeClosedInt((Integer) left, (Integer) right);
//        }
//        if (left instanceof Long && right instanceof Long) {
//            return (Optional) rangeClosedLong((Long) left, (Long) right);
//        }
//        if (left instanceof Byte && right instanceof Byte) {
//            return (Optional) rangeClosedByte((Byte) left, (Byte) right);
//        }
//        if (left instanceof Character && right instanceof Character) {
//            return (Optional) rangeClosedCharacter((Character) left, (Character) right);
//        }
//        if (left instanceof Short && right instanceof Short) {
//            return (Optional) rangeClosedShort((Short) left, (Short) right);
//        }
//        if (left instanceof Float && right instanceof Float) {
//            return (Optional) rangeClosedFloat((Float) left, (Float) right);
//        }
//        if (left instanceof Double && right instanceof Double) {
//            return (Optional) rangeClosedDouble((Double) left, (Double) right);
//        }
//        if (left instanceof BigInteger && right instanceof BigInteger) {
//            return (Optional) rangeClosedBigInteger((BigInteger) left, (BigInteger) right);
//        }
//        if (left instanceof BigDecimal && right instanceof BigDecimal) {
//            return (Optional) rangeClosedBigDecimal((BigDecimal) left, (BigDecimal) right);
//        }
//        if (left instanceof LocalDate && right instanceof LocalDate) {
//            return (Optional) rangeClosedLocalDate((LocalDate) left, (LocalDate) right);
//        }
//        if (left instanceof LocalDateTime && right instanceof LocalDateTime) {
//            return (Optional) rangeClosedLocalDateTime((LocalDateTime) left, (LocalDateTime) right);
//        }
//        if (left instanceof LocalTime && right instanceof LocalTime) {
//            return (Optional) rangeClosedLocalTime((LocalTime) left, (LocalTime) right);
//        }
//        if (left instanceof java.sql.Date && right instanceof Date ) {
//            return (Optional) rangeClosedLocalDate( ((Date) left).toLocalDate(), ((Date) right).toLocalDate());
//        }
//        if (left instanceof java.sql.Time && right instanceof java.sql.Time ) {
//            return (Optional) rangeClosedLocalTime( ((java.sql.Time) left).toLocalTime(), ((java.sql.Time) right).toLocalTime());
//        }
//        if (left instanceof java.sql.Timestamp && right instanceof java.sql.Timestamp ) {
//            return (Optional) rangeClosedLocalDateTime( ((java.sql.Timestamp) left).toLocalDateTime(), ((java.sql.Timestamp) right).toLocalDateTime());
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<LocalTime>>  rangeClosedLocalTime(LocalTime left, LocalTime right) {
//        TemporalAmount temporalAmount =  (this.unit == null)? Duration.ofSeconds(1):(TemporalAmount)this.unit;
//        if (right.compareTo(left)>0){
//            LocalTime cur = left;
//            ArrayList<LocalTime> res = new ArrayList<>();
//            for (int i = 0; i <= size; i++) {
//                res.add(cur);
//                cur.plus(temporalAmount);
//            }
//            return Optional.ofNullable(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<LocalDateTime>> rangeClosedLocalDateTime(LocalDateTime left, LocalDateTime right) {
//        TemporalAmount temporalAmount =  (this.unit == null)?Period.ofDays(1):(TemporalAmount)this.unit;
//        if (right.compareTo(left)>0){
//            LocalDateTime cur = left;
//            ArrayList<LocalDateTime> res = new ArrayList<>();
//            for (int i = 0; i <= size; i++) {
//                res.add(cur);
//                cur.plus(temporalAmount);
//            }
//            return Optional.ofNullable(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<LocalDate>> rangeClosedLocalDate(LocalDate left, LocalDate right) {
//        TemporalAmount temporalAmount =  (this.unit == null)?Period.ofDays(1):(TemporalAmount)this.unit;
//        if (right.compareTo(left)>0){
//            LocalDate cur = left;
//            ArrayList<LocalDate> res = new ArrayList<>();
//            for (int i = 0; i <= size; i++) {
//                res.add(cur);
//                cur.plus(temporalAmount);
//            }
//            return Optional.ofNullable(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<BigInteger>> rangeClosedBigInteger(BigInteger left, BigInteger right) {
//        int limit = right.intValue();
//        int start = left.intValue();
//        int diff = -left.intValue();
//        if (start > 0 && limit > 0 && size > diff && diff > 0) {
//            ArrayList<BigInteger> res = new ArrayList<>();
//            for (int i = 0; i <= diff; i++) {
//                res.add(left.add(BigInteger.ONE));
//            }
//            return Optional.of(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<BigDecimal>> rangeClosedBigDecimal(BigDecimal left, BigDecimal right) {
//        int limit = right.intValue();
//        int start = left.intValue();
//        int diff = -left.intValue();
//        if (start > 0 && limit > 0 && size > diff && diff > 0) {
//            ArrayList<BigDecimal> res = new ArrayList<>();
//            for (int i = 0; i <= diff; i++) {
//                res.add(left.add(BigDecimal.ONE));
//            }
//            return Optional.of(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<Float>> rangeClosedFloat(Float left, Float right) {
//        double diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            ArrayList<Float> res = new ArrayList<>();
//            for (float i = left; i <= right; i++) {
//                res.add(i);
//            }
//            return Optional.of(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<Double>> rangeClosedDouble(Double left, Double right) {
//        double diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            ArrayList<Double> res = new ArrayList<>();
//            for (double i = left; i <= right; i++) {
//                res.add(i);
//            }
//            return Optional.of(res);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<Short>> rangeClosedShort(Short left, Short right) {
//        long diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            return Optional.of(() -> (Iterator) IntStream.rangeClosed(left, right).mapToObj(i -> Short.valueOf((short) i)).iterator());
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<Character>> rangeClosedCharacter(Character left, Character right) {
//        long diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            return Optional.of(() -> (Iterator) IntStream.rangeClosed(left, right).mapToObj(i -> Character.valueOf((char) i)).iterator());
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<Byte>> rangeClosedByte(byte left, byte right) {
//        long diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            return Optional.of(() -> (Iterator) IntStream.rangeClosed(left, right).mapToObj(i -> Byte.valueOf((byte) i)).iterator());
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Iterable<Byte>> rangeClosedLong(long left, long right) {
//        long diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            return Optional.of(() -> (Iterator) LongStream.rangeClosed(left, right).boxed().iterator());
//        }
//        return Optional.empty();
//    }
//
//    public Optional<Iterable<Byte>> rangeClosedInt(int left, int right) {
//        int diff = right - left;
//        if (left > 0 && right > 0 && size > diff && diff > 0) {
//            return Optional.of(() -> (Iterator) IntStream.rangeClosed(left, right).boxed().iterator());
//        }
//        return Optional.empty();
//    }
//
//    public Optional<List<Integer>> range(int left, int right) {
//        int diff = right - left;
//        if (size > diff && diff > 0) {
//            ArrayList<Integer> res = new ArrayList<>();
//            for (int i = left; i <= right; i++) {
//                res.add(i);
//            }
//            return Optional.of(res);
//        }
//        return Optional.empty();
//    }
}
