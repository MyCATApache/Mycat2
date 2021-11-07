package io.mycat.router.range;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class IntEnumerator extends Enumerator<Integer, Integer> {
    public IntEnumerator(int size, Integer start, Integer end, Integer unit, boolean cycle, int enumSize) {
        super(size, start, end, unit, cycle, enumSize);
    }

    @Override
    public Optional<Iterable<Integer>> rangeClosed(Integer left, Integer right) {
        int diff = right > left ? right - left : size - left + right;
        if (diff > enumSize) {
            return Optional.empty();
        }
        if (left < start) {
            return Optional.empty();
        }
        if (right > end) {
            return Optional.empty();
        }
        if (!(left >= 0 && size >= left)) {
            return Optional.empty();
        }
        if (!(right >= 0 && size >= right)) {
            return Optional.empty();
        }
        List<Integer> list = new ArrayList<>();
        int cur = left;
        for (int i = 0; i <= diff; i++) {
            list.add(cur);
            cur += unit;
            if (cur > end) {
                if (cycle) {
                    cur = start;
                } else {
                    break;
                }
            }
        }
        return Optional.of(list);
    }

    public static IntEnumerator ofInt(int start, int end, int unit, boolean cycle, int enumSize) {
        return new IntEnumerator(end - start + 1, start, end, unit, cycle, enumSize);
    }

}
