package io.mycat.calcite.sqlfunction;

import com.alibaba.druid.util.Utils;
import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

public class EncryptionHashingCompressionFunctions {

    /**
     * MD5
     * Syntax
     * MD5(str)
     * Description
     * Calculates an MD5 128-bit checksum for the string.
     *
     * The return value is a 32-hex digit string, and as of MariaDB 5.5, is a nonbinary string in the connection character set and collation, determined by the values of the character_set_connection and collation_connection system variables. Before 5.5, the return value was a binary string.
     *
     * NULL is returned if the argument was NULL.
     *
     * Examples
     * SELECT MD5('testing');
     * +----------------------------------+
     * | MD5('testing')                   |
     * +----------------------------------+
     * | ae2b1fca515949e5d54fb22b8ed95575 |
     * +----------------------------------+
     *
     * @param param0Value
     * @return
     */
    public static String MD5(String param0Value) {
        if (param0Value == null){
            return null;
        }
        return Utils .md5(param0Value);
    }

    /**
     * UNCOMPRESS
     * Syntax
     * UNCOMPRESS(string_to_uncompress)
     * Description
     * Uncompresses a string compressed by the COMPRESS() function. If the argument is not a compressed value, the result is NULL. This function requires MariaDB to have been compiled with a compression library such as zlib. Otherwise, the return value is always NULL. The have_compress server system variable indicates whether a compression library is present.
     * <p>
     * Examples
     * SELECT UNCOMPRESS(COMPRESS('a string'));
     * +----------------------------------+
     * | UNCOMPRESS(COMPRESS('a string')) |
     * +----------------------------------+
     * | a string                         |
     * +----------------------------------+
     * <p>
     * SELECT UNCOMPRESS('a string');
     * +------------------------+
     * | UNCOMPRESS('a string') |
     * +------------------------+
     * | NULL                   |
     * +------------------------+
     *
     * @param param0Value
     * @return
     */
    public static String UNCOMPRESS(String param0Value) {
        Object uncompress = UnsolvedMysqlFunctionUtil.eval("UNCOMPRESS", param0Value);
        if (uncompress == null) {
            return null;
        }
        return uncompress.toString();
    }

    /**
     * UNCOMPRESSED_LENGTH
     * Syntax
     * UNCOMPRESSED_LENGTH(compressed_string)
     * Description
     * Returns the length that the compressed string had before being compressed with COMPRESS().
     * <p>
     * UNCOMPRESSED_LENGTH() returns NULL or an incorrect result if the string is not compressed.
     * <p>
     * Until MariaDB 10.3.1, returns MYSQL_TYPE_LONGLONG, or bigint(10), in all cases. From MariaDB 10.3.1, returns MYSQL_TYPE_LONG, or int(10), when the result would fit within 32-bits.
     * <p>
     * Examples
     * SELECT UNCOMPRESSED_LENGTH(COMPRESS(REPEAT('a',30)));
     * +-----------------------------------------------+
     * | UNCOMPRESSED_LENGTH(COMPRESS(REPEAT('a',30))) |
     * +-----------------------------------------------+
     * |                                            30 |
     * +-----------------------------------------------+
     *
     * @param arg0
     * @return
     */
    public static Integer UNCOMPRESSED_LENGTH(String arg0) {
        if (arg0 == null) {
            return null;
        }
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("UNCOMPRESSED_LENGTH", arg0).toString());
    }
}