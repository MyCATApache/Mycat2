package io.mycat.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;


/**
 * Unsafe 工具类
 *
 * @author zagnix
 * @create 2016-11-18 14:17
 */
public final class UnsafeMemory {
    private final static Logger logger = LoggerFactory.getLogger(UnsafeMemory.class);
    private static final Unsafe _UNSAFE;
    public static final int BYTE_ARRAY_OFFSET;
    static {
        Unsafe unsafe;
        try {
            Field theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            unsafe = (Unsafe) theUnsafeField.get(null);
        } catch (Exception e) {
           logger.error(e.getMessage());
            unsafe = null;
        }
        _UNSAFE = unsafe;
        if (_UNSAFE != null) {
            BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
        } else {
            BYTE_ARRAY_OFFSET = 0;
        }
    }
    public static Unsafe getUnsafe() {
        return _UNSAFE;
    }

    /**
     * 将size规整化为pagesize的倍数
     * @param size
     * @return
     */
    public static long roundToOsPageSzie(long size) {
        long pagesize = _UNSAFE.pageSize();
        return (size + (pagesize-1)) & ~(pagesize-1);

    }

}
