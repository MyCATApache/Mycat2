package io.mycat.router.range;

import java.util.ArrayList;
import java.util.List;


/**
 * chenjunwen nange
 */
public interface Range {

    int start();

    int end();

    default int size() {
        return end() - start() + 1;
    }

    public static Range create(int start, int end) {
        return new RangeImpl(start, end);
    }

    default List<Range> removeAndGetRemain(Range newRange) {
        List<Range> result = new ArrayList<>();
        if (newRange.start() > this.end() || newRange.end() < this.start()) {
            result.add(this);
        } else if (newRange.start() <= this.start() && newRange.end() >= this.end()) {
            return result;
        } else if (newRange.start() > this.start() && newRange.end() < this.end()) {
            result.add(create(this.start(), newRange.start() - 1));
            result.add(create(newRange.end() + 1, this.end()));
        } else if (newRange.start() <= this.start() && newRange.end() < this.end()) {
            result.add(create(newRange.end() + 1, this.end()));
        } else if (newRange.start() > this.start() && newRange.end() >= this.end()) {
            result.add(create(this.start(), newRange.start() - 1));
        }
        return result;
    }

    static List<Range> removeAndGetRemain(List<Range> oriRangeList, Range newRange) {
        List<Range> result = new ArrayList<>();
        for (Range range : oriRangeList) {
            result.addAll(range.removeAndGetRemain(newRange));
        }
        return result;
    }

    static List<Range> removeAndGetRemain(List<Range> oriRangeList, List<Range> rangeList) {
        for (Range range : rangeList) {
            oriRangeList = removeAndGetRemain(oriRangeList, range);
        }
        return oriRangeList;
    }

    public static class RangeImpl implements Range {
        final int start;
        final int end;

        public RangeImpl(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int start() {
            return start;
        }

        @Override
        public int end() {
            return end;
        }

    }
}