package io.mycat.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;

import io.mycat.util.RandomUtil;
import io.mycat.util.SecurityUtil;

/**
 * ${todo}
 *
 * @author : zhuqiang
 * @date : 2018/11/25 20:03
 */
public class MysqlNativePasswordPluginUtil {
    private static Logger logger = LoggerFactory.getLogger(MysqlNativePasswordPluginUtil.class);
    public static final String PROTOCOL_PLUGIN_NAME = "mysql_native_password";

    public static byte[] scramble411(String password, byte[] seed) {
        if (password == null || password.length() == 0) {
            logger.warn("password is empty");
            return null;
        }
        try {
            return SecurityUtil.scramble411(password.getBytes(), seed);
        } catch (NoSuchAlgorithmException e) {
            logger.warn("no such algorithm", e);
            return null;
        }
    }

    public static byte[] scramble411(String password, String seed) {
        return scramble411(password, seed.getBytes());
    }

    public static byte[][] nextSeedBuild() {
        byte[] authPluginDataPartOne = RandomUtil.randomBytes(8);
        byte[] authPluginDataPartTwo = RandomUtil.randomBytes(12);

        // 保存认证数据
        byte[] seed = new byte[20];
        System.arraycopy(authPluginDataPartOne, 0, seed, 0, 8);
        System.arraycopy(authPluginDataPartTwo, 0, seed, 8, 12);

        byte[][] result = new byte[3][1];
        result[0] = authPluginDataPartOne;
        result[1] = authPluginDataPartTwo;
        result[2] = seed;
        return result;
    }

    public static String[] nextSeedStringBuild() {
        byte[][] bytes = nextSeedBuild();
        return new String[]{
                new String(bytes[0]),
                new String(bytes[1]),
                new String(bytes[2]),
        };
    }
}
