package io.mycat.ratelimiter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IntContext implements ComparedRateLimiterService.Context {
    AtomicLong sum;

    public IntContext(int sum) {
        this.sum = new AtomicLong(sum);
    }

    public static class IntItem implements ComparedRateLimiterService.ContextItem {
        int value;
    }
    public static class IntItemLimit implements ComparedRateLimiterService.ContextLimit {
        int limit;

        @Override
        public boolean tryLimit(ComparedRateLimiterService.ContextItem context) {
            return limit>((IntItem)context).value;
        }
    }

    @Override
    public boolean tryMinus(ComparedRateLimiterService.ContextItem context) {
        IntItem intItem = (IntItem) context;
        AtomicBoolean res = new AtomicBoolean();
        this.sum.updateAndGet(operand -> {
            long cur = sum.get();
            long l = cur - intItem.value;
            if (l >= 0) {
                res.set(true);
                return l;
            } else {
                res.set(false);
                return operand;
            }
        });
        return res.get();
    }

    @Override
    public void recovery(ComparedRateLimiterService.ContextItem context) {
        IntItem intItem = (IntItem) context;
        this.sum.getAndUpdate(operand -> operand + intItem.value);
    }
}
