package io.mycat.calcite.sqlfunction;

import java.math.BigInteger;
import java.util.UUID;

public class MysqlControlFlowFunctions {

    public static BigInteger UUID_SHORT() {
        return  new BigInteger(Long.toUnsignedString(UUID.randomUUID().getLeastSignificantBits()));
    }
    public static String UUID() {
        return  (UUID.randomUUID().toString());
    }
}