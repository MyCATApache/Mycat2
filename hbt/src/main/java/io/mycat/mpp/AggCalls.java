package io.mycat.mpp;

import java.math.BigInteger;

public class AggCalls {
    interface AggCall {

        public void accept(long value);

        public long getValue();

        public void reset();

       default public Class type(){
            return Long.TYPE;
        }
    }
}