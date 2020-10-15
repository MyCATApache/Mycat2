package io.mycat.calcite.sqlfunction;

import java.math.BigInteger;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MysqlOtherFunctions {

    public static double RAND() {
        return ThreadLocalRandom.current().nextDouble();
    }
    public static BigInteger UUID_SHORT() {
        return  new BigInteger(Long.toUnsignedString(UUID.randomUUID().getLeastSignificantBits()));
    }
    public static String UUID() {
        return  (UUID.randomUUID().toString());
    }
}