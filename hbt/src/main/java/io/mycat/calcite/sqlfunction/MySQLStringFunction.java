package io.mycat.calcite.sqlfunction;

import com.alibaba.druid.sql.visitor.functions.OneParamFunctions;
import com.alibaba.druid.util.Base64;
import com.alibaba.druid.util.HexBin;
import com.alibaba.fastsql.util.StringUtils;
import io.mycat.calcite.UnsolvedMysqlFunctionUtil;
import org.apache.calcite.avatica.util.ByteString;

import java.util.Arrays;

public class MySQLStringFunction {

    /**
     * Open Questions
     * MariaDB Server
     * MariaDB MaxScale
     * MariaDB ColumnStore
     * Connectors
     * Created
     * 10 years, 3 months ago
     * Modified
     * 1 year, 8 months ago
     * Type
     * article
     * Status
     * active
     * License
     * GPLv2 fill_help_tables.sql
     * History
     * Comments
     * Edit
     * Attachments
     * No attachments exist
     * Localized Versions
     * ASCII [it]
     * ASCII
     * Syntax
     * ASCII(str)
     * Description
     * Returns the numeric ASCII value of the leftmost character of the string argument. Returns 0 if the given string is empty and NULL if it is NULL.
     * <p>
     * ASCII() works for 8-bit characters.
     * <p>
     * Examples
     * SELECT ASCII(9);
     * +----------+
     * | ASCII(9) |
     * +----------+
     * |       57 |
     * +----------+
     * <p>
     * SELECT ASCII('9');
     * +------------+
     * | ASCII('9') |
     * +------------+
     * |         57 |
     * +------------+
     * <p>
     * SELECT ASCII('abc');
     * +--------------+
     * | ASCII('abc') |
     * +--------------+
     * |           97 |
     * +--------------+
     *
     * @param a
     * @return
     */
    public static Integer ASCII(Object a) {
        if (a == null) {
            return null;
        }
        if ("".equals(a)) {
            return 0;
        }
        return a.toString().codePointAt(0);
    }

    /**
     * BIN
     * Syntax
     * BIN(N)
     * Description
     * Returns a string representation of the binary value of the given longlong (that is, BIGINT) number. This is equivalent to CONV(N,10,2). The argument should be positive. If it is a FLOAT, it will be truncated. Returns NULL if the argument is NULL.
     * <p>
     * Examples
     * SELECT BIN(12);
     * +---------+
     * | BIN(12) |
     * +---------+
     * | 1100    |
     * +---------+
     *
     * @param a
     * @return
     */
    public static String BIN(Long a) {
        if (a == null) {
            return null;
        }
        return Long.toBinaryString(a);
    }

    /**
     * BINARY Operator
     * Syntax
     * BINARY
     * Description
     * The BINARY operator casts the string following it to a binary string. This is an easy way to force a column comparison to be done byte by byte rather than character by character. This causes the comparison to be case sensitive even if the column isn't defined as BINARY or BLOB.
     * <p>
     * BINARY also causes trailing spaces to be significant.
     * <p>
     * Examples
     * SELECT 'a' = 'A';
     * +-----------+
     * | 'a' = 'A' |
     * +-----------+
     * |         1 |
     * +-----------+
     * <p>
     * SELECT BINARY 'a' = 'A';
     * +------------------+
     * | BINARY 'a' = 'A' |
     * +------------------+
     * |                0 |
     * +------------------+
     * <p>
     * SELECT 'a' = 'a ';
     * +------------+
     * | 'a' = 'a ' |
     * +------------+
     * |          1 |
     * +------------+
     * <p>
     * SELECT BINARY 'a' = 'a ';
     * +-------------------+
     * | BINARY 'a' = 'a ' |
     * +-------------------+
     * |                 0 |
     * +-------------------+
     *
     * @param a
     * @return
     */
    public static ByteString BINARY(String a) {
        if (a == null) {
            return null;
        }
        return ByteString.of(a, 2);
    }


    /**
     * BIT_LENGTH
     * Syntax
     * BIT_LENGTH(str)
     * Contents
     * Syntax
     * Description
     * Examples
     * Compatibility
     * Description
     * Returns the length of the given string argument in bits. If the argument is not a string, it will be converted to string. If the argument is NULL, it returns NULL.
     * <p>
     * Examples
     * SELECT BIT_LENGTH('text');
     * +--------------------+
     * | BIT_LENGTH('text') |
     * +--------------------+
     * |                 32 |
     * +--------------------+
     * SELECT BIT_LENGTH('');
     * +----------------+
     * | BIT_LENGTH('') |
     * +----------------+
     * |              0 |
     * +----------------+
     * Compatibility
     * PostgreSQL and Sybase support BIT_LENGTH().
     *
     * @param a
     * @return
     */
    public static Integer bit_length(String a) {
        if (a == null) {
            return null;
        }
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("bit_length", a).toString());
    }

    //cast

    /**
     * CAST
     * Syntax
     * CAST(expr AS type)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * The CAST() function takes a value of one type and produces a value of another type, similar to the CONVERT() function. For more information, see the description of CONVERT().
     *
     * The main difference between the CAST() and CONVERT() is that CONVERT(expr,type) is ODBC syntax while CAST(expr as type) and CONVERT(... USING ...) are SQL92 syntax.
     *
     * In MariaDB 10.4 and later, you can use the CAST() function with the INTERVAL keyword.
     *
     * MariaDB starting with 5.5.31
     * Until MariaDB 5.5.31, X'HHHH', the standard SQL syntax for binary string literals, erroneously worked in the same way as 0xHHHH. In 5.5.31 it was intentionally changed to behave as a string in all contexts (and never as a number).
     *
     * This introduces an incompatibility with previous versions of MariaDB, and all versions of MySQL (see the example below).
     *
     * Examples
     * Simple casts:
     *
     * SELECT CAST("abc" AS BINARY);
     * SELECT CAST("1" AS UNSIGNED INTEGER);
     * SELECT CAST(123 AS CHAR CHARACTER SET utf8)
     * Note that when one casts to CHAR without specifying the character set, the collation_connection character set collation will be used. When used with CHAR CHARACTER SET, the default collation for that character set will be used.
     *
     * SELECT COLLATION(CAST(123 AS CHAR));
     * +------------------------------+
     * | COLLATION(CAST(123 AS CHAR)) |
     * +------------------------------+
     * | latin1_swedish_ci            |
     * +------------------------------+
     *
     * SELECT COLLATION(CAST(123 AS CHAR CHARACTER SET utf8));
     * +-------------------------------------------------+
     * | COLLATION(CAST(123 AS CHAR CHARACTER SET utf8)) |
     * +-------------------------------------------------+
     * | utf8_general_ci                                 |
     * +-------------------------------------------------+
     * If you also want to change the collation, you have to use the COLLATE operator:
     *
     * SELECT COLLATION(CAST(123 AS CHAR CHARACTER SET utf8)
     *   COLLATE utf8_unicode_ci);
     * +-------------------------------------------------------------------------+
     * | COLLATION(CAST(123 AS CHAR CHARACTER SET utf8) COLLATE utf8_unicode_ci) |
     * +-------------------------------------------------------------------------+
     * | utf8_unicode_ci                                                         |
     * +-------------------------------------------------------------------------+
     * Using CAST() to order an ENUM field as a CHAR rather than the internal numerical value:
     *
     * CREATE TABLE enum_list (enum_field enum('c','a','b'));
     *
     * INSERT INTO enum_list (enum_field)
     * VALUES('c'),('a'),('c'),('b');
     *
     * SELECT * FROM enum_list
     * ORDER BY enum_field;
     * +------------+
     * | enum_field |
     * +------------+
     * | c          |
     * | c          |
     * | a          |
     * | b          |
     * +------------+
     *
     * SELECT * FROM enum_list
     * ORDER BY CAST(enum_field AS CHAR);
     * +------------+
     * | enum_field |
     * +------------+
     * | a          |
     * | b          |
     * | c          |
     * | c          |
     * +------------+
     * From MariaDB 5.5.31, the following will trigger warnings, since x'aa' and 'X'aa' no longer behave as a number. Previously, and in all versions of MySQL, no warnings are triggered since they did erroneously behave as a number:
     *
     * SELECT CAST(0xAA AS UNSIGNED), CAST(x'aa' AS UNSIGNED), CAST(X'aa' AS UNSIGNED);
     * +------------------------+-------------------------+-------------------------+
     * | CAST(0xAA AS UNSIGNED) | CAST(x'aa' AS UNSIGNED) | CAST(X'aa' AS UNSIGNED) |
     * +------------------------+-------------------------+-------------------------+
     * |                    170 |                       0 |                       0 |
     * +------------------------+-------------------------+-------------------------+
     * 1 row in set, 2 warnings (0.00 sec)
     *
     * Warning (Code 1292): Truncated incorrect INTEGER value: '\xAA'
     * Warning (Code 1292): Truncated incorrect INTEGER value: '\xAA'
     * Casting to intervals:
     *
     * SELECT CAST(2019-01-04 INTERVAL AS DAY_SECOND(2)) AS "Cast";
     *
     * +-------------+
     * | Cast        |
     * +-------------+
     * | 00:20:17.00 |
     * +-------------+
     */


    /**
     * CHAR Function
     * Syntax
     * CHAR(N,... [USING charset_name])
     * Description
     * CHAR() interprets each argument as an INT and returns a string consisting of the characters given by the code values of those integers. NULL values are skipped. By default, CHAR() returns a binary string. To produce a string in a given character set, use the optional USING clause:
     * <p>
     * SELECT CHARSET(CHAR(0x65)), CHARSET(CHAR(0x65 USING utf8));
     * +---------------------+--------------------------------+
     * | CHARSET(CHAR(0x65)) | CHARSET(CHAR(0x65 USING utf8)) |
     * +---------------------+--------------------------------+
     * | binary              | utf8                           |
     * +---------------------+--------------------------------+
     * If USING is given and the result string is illegal for the given character set, a warning is issued. Also, if strict SQL mode is enabled, the result from CHAR() becomes NULL.
     * <p>
     * Examples
     * SELECT CHAR(77,97,114,'105',97,'68',66);
     * +----------------------------------+
     * | CHAR(77,97,114,'105',97,'68',66) |
     * +----------------------------------+
     * | MariaDB                          |
     * +----------------------------------+
     * <p>
     * SELECT CHAR(77,77.3,'77.3');
     * +----------------------+
     * | CHAR(77,77.3,'77.3') |
     * +----------------------+
     * | MMM                  |
     * +----------------------+
     * 1 row in set, 1 warning (0.00 sec)
     * <p>
     * Warning (Code 1292): Truncated incorrect INTEGER value: '77.3'
     * See Also
     * Character Sets and Collations
     * ASCII() - Return ASCII value of first character
     * ORD() - Return value for character in single or multi-byte character sets
     * CHR - Similar, Oracle-compatible, function
     *
     * @param c
     * @return
     */
    public static String CHAR(Object... c) {
        if (c == null) {
            return null;
        }
        return UnsolvedMysqlFunctionUtil.eval("char", c).toString();
    }


    /**
     * CHARACTER_LENGTH
     * Syntax
     * CHARACTER_LENGTH(str)
     * Description
     * CHARACTER_LENGTH() is a synonym for CHAR_LENGTH().
     *
     * @param c
     * @return
     */
    public static Integer CHARACTER_LENGTH(String c) {
        return CHAR_LENGTH(c);
    }

    /**
     * CHAR_LENGTH
     * Syntax
     * CHAR_LENGTH(str)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the length of the given string argument, measured in characters. A multi-byte character counts as a single character. This means that for a string containing five two-byte characters, LENGTH() (or OCTET_LENGTH() in Oracle mode) returns 10, whereas CHAR_LENGTH() returns 5. If the argument is NULL, it returns NULL.
     * <p>
     * If the argument is not a string value, it is converted into a string.
     * <p>
     * It is synonymous with the CHARACTER_LENGTH() function.
     * <p>
     * Until MariaDB 10.3.1, returns MYSQL_TYPE_LONGLONG, or bigint(10), in all cases. From MariaDB 10.3.1, returns MYSQL_TYPE_LONG, or int(10), when the result would fit within 32-bits.
     * <p>
     * Examples
     * SELECT CHAR_LENGTH('MariaDB');
     * +------------------------+
     * | CHAR_LENGTH('MariaDB') |
     * +------------------------+
     * |                      7 |
     * +------------------------+
     * <p>
     * SELECT CHAR_LENGTH('π');
     * +-------------------+
     * | CHAR_LENGTH('π')  |
     * +-------------------+
     * |                 1 |
     * +-------------------+
     * See Also¶
     * LENGTH()
     * OCTET_LENGTH()
     * Oracle mode from MariaDB 10.3
     *
     * @param c
     * @return
     */
    public static Integer CHAR_LENGTH(String c) {
        if (c == null) {
            return null;
        }
        return c.length();
    }

    /**
     * CHR
     * MariaDB starting with 10.3.1
     * The CHR() function was introduced in MariaDB 10.3.1 to provide Oracle compatibility
     * <p>
     * Syntax
     * CHR(N)
     * Description
     * CHR() interprets each argument N as an integer and returns a VARCHAR(1) string consisting of the character given by the code values of the integer. The character set and collation of the string are set according to the values of the character_set_database and collation_database system variables.
     * <p>
     * CHR() is similar to the CHAR() function, but only accepts a single argument.
     * <p>
     * CHR() is available in all sql_modes.
     * <p>
     * Examples
     * SELECT CHR(67);
     * +---------+
     * | CHR(67) |
     * +---------+
     * | C       |
     * +---------+
     * <p>
     * SELECT CHR('67');
     * +-----------+
     * | CHR('67') |
     * +-----------+
     * | C         |
     * +-----------+
     * <p>
     * SELECT CHR('C');
     * +----------+
     * | CHR('C') |
     * +----------+
     * |          |
     * +----------+
     * 1 row in set, 1 warning (0.000 sec)
     * <p>
     * SHOW WARNINGS;
     * +---------+------+----------------------------------------+
     * | Level   | Code | Message                                |
     * +---------+------+----------------------------------------+
     * | Warning | 1292 | Truncated incorrect INTEGER value: 'C' |
     * +---------+------+----------------------------------------+
     * See Also
     * Character Sets and Collations
     * ASCII() - Return ASCII value of first character
     * ORD() - Return value for character in single or multi-byte character sets
     * CHAR() - Similar function which accepts multiple integers
     *
     * @param n
     * @return
     */
    public static String chr(long n) {
        return String.valueOf((char) n);
    }

    /**
     * CONCAT
     * Syntax
     * CONCAT(str1,str2,...)
     * Contents
     * Syntax
     * Description
     * Oracle Mode
     * Examples
     * See Also
     * Description
     * Returns the string that results from concatenating the arguments. May have one or more arguments. If all arguments are non-binary strings, the result is a non-binary string. If the arguments include any binary strings, the result is a binary string. A numeric argument is converted to its equivalent binary string form; if you want to avoid that, you can use an explicit type cast, as in this example:
     * <p>
     * SELECT CONCAT(CAST(int_col AS CHAR), char_col);
     * CONCAT() returns NULL if any argument is NULL.
     * <p>
     * A NULL parameter hides all information contained in other parameters from the result. Sometimes this is not desirable; to avoid this, you can:
     * <p>
     * Use the CONCAT_WS() function with an empty separator, because that function is NULL-safe.
     * Use IFNULL() to turn NULLs into empty strings.
     * Oracle Mode
     * MariaDB starting with 10.3
     * In Oracle mode from MariaDB 10.3, CONCAT ignores NULL.
     * <p>
     * Examples
     * SELECT CONCAT('Ma', 'ria', 'DB');
     * +---------------------------+
     * | CONCAT('Ma', 'ria', 'DB') |
     * +---------------------------+
     * | MariaDB                   |
     * +---------------------------+
     * <p>
     * SELECT CONCAT('Ma', 'ria', NULL, 'DB');
     * +---------------------------------+
     * | CONCAT('Ma', 'ria', NULL, 'DB') |
     * +---------------------------------+
     * | NULL                            |
     * +---------------------------------+
     * <p>
     * SELECT CONCAT(42.0);
     * +--------------+
     * | CONCAT(42.0) |
     * +--------------+
     * | 42.0         |
     * +--------------+
     * Using IFNULL() to handle NULLs:
     * <p>
     * SELECT CONCAT('The value of @v is: ', IFNULL(@v, ''));
     * +------------------------------------------------+
     * | CONCAT('The value of @v is: ', IFNULL(@v, '')) |
     * +------------------------------------------------+
     * | The value of @v is:                            |
     * +------------------------------------------------+
     * In Oracle mode, from MariaDB 10.3:
     * <p>
     * SELECT CONCAT('Ma', 'ria', NULL, 'DB');
     * +---------------------------------+
     * | CONCAT('Ma', 'ria', NULL, 'DB') |
     * +---------------------------------+
     * | MariaDB                         |
     * +---------------------------------+
     * See Also
     * GROUP_CONCAT()
     * Oracle mode from MariaDB 10.3
     *
     * @param n
     * @return
     */
    public static String CONCAT(String... n) {
        return String.join("", n);
    }

    /**
     * Open Questions
     * MariaDB Server
     * MariaDB MaxScale
     * MariaDB ColumnStore
     * Connectors
     * Created
     * 10 years, 3 months ago
     * Modified
     * 1 year, 8 months ago
     * Type
     * article
     * Status
     * active
     * License
     * GPLv2 fill_help_tables.sql
     * History
     * Comments
     * Edit
     * Attachments
     * No attachments exist
     * Localized Versions
     * CONCAT_WS [it]
     * CONCAT_WS
     * Syntax
     * CONCAT_WS(separator,str1,str2,...)
     * Description
     * CONCAT_WS() stands for Concatenate With Separator and is a special form of CONCAT(). The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments.
     * <p>
     * If the separator is NULL, the result is NULL; all other NULL values are skipped. This makes CONCAT_WS() suitable when you want to concatenate some values and avoid losing all information if one of them is NULL.
     * <p>
     * Examples
     * SELECT CONCAT_WS(',','First name','Second name','Last Name');
     * +-------------------------------------------------------+
     * | CONCAT_WS(',','First name','Second name','Last Name') |
     * +-------------------------------------------------------+
     * | First name,Second name,Last Name                      |
     * +-------------------------------------------------------+
     * <p>
     * SELECT CONCAT_WS('-','Floor',NULL,'Room');
     * +------------------------------------+
     * | CONCAT_WS('-','Floor',NULL,'Room') |
     * +------------------------------------+
     * | Floor-Room                         |
     * +------------------------------------+
     * In some cases, remember to include a space in the separator string:
     * <p>
     * SET @a = 'gnu', @b = 'penguin', @c = 'sea lion';
     * Query OK, 0 rows affected (0.00 sec)
     * <p>
     * SELECT CONCAT_WS(', ', @a, @b, @c);
     * +-----------------------------+
     * | CONCAT_WS(', ', @a, @b, @c) |
     * +-----------------------------+
     * | gnu, penguin, sea lion      |
     * +-----------------------------+
     * Using CONCAT_WS() to handle NULLs:
     * <p>
     * SET @a = 'a', @b = NULL, @c = 'c';
     * <p>
     * SELECT CONCAT_WS('', @a, @b, @c);
     * +---------------------------+
     * | CONCAT_WS('', @a, @b, @c) |
     * +---------------------------+
     * | ac                        |
     * +---------------------------+
     * See Also
     * GROUP_CONCAT()
     *
     * @param n
     * @return
     */
    public static String CONCAT_WS(String... n) {
        return String.join(n[0], Arrays.copyOfRange(n, 1, n.length));
    }

    //CONVERT
    /**
     * CONVERT
     * Syntax
     * CONVERT(expr,type), CONVERT(expr USING transcoding_name)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * The CONVERT() and CAST() functions take a value of one type and produce a value of another type.
     *
     * The type can be one of the following values:
     *
     * BINARY
     * CHAR
     * DATE
     * DATETIME
     * DECIMAL[(M[,D])]
     * DOUBLE
     * FLOAT — From MariaDB 10.4.5
     * INTEGER
     * Short for SIGNED INTEGER
     * SIGNED [INTEGER]
     * TIME
     * UNSIGNED [INTEGER]
     * Note that in MariaDB, INT and INTEGER are the same thing.
     *
     * BINARY produces a string with the BINARY data type. If the optional length is given, BINARY(N) causes the cast to use no more than N bytes of the argument. Values shorter than the given number in bytes are padded with 0x00 bytes to make them equal the length value.
     *
     * CHAR(N) causes the cast to use no more than the number of characters given in the argument.
     *
     * The main difference between the CAST() and CONVERT() is that CONVERT(expr,type) is ODBC syntax while CAST(expr as type) and CONVERT(... USING ...) are SQL92 syntax.
     *
     * CONVERT() with USING is used to convert data between different character sets. In MariaDB, transcoding names are the same as the corresponding character set names. For example, this statement converts the string 'abc' in the default character set to the corresponding string in the utf8 character set:
     *
     * SELECT CONVERT('abc' USING utf8);
     * Examples
     * SELECT enum_col FROM tbl_name
     * ORDER BY CAST(enum_col AS CHAR);
     * Converting a BINARY to string to permit the LOWER function to work:
     *
     * SET @x = 'AardVark';
     *
     * SET @x = BINARY 'AardVark';
     *
     * SELECT LOWER(@x), LOWER(CONVERT (@x USING latin1));
     * +-----------+----------------------------------+
     * | LOWER(@x) | LOWER(CONVERT (@x USING latin1)) |
     * +-----------+----------------------------------+
     * | AardVark  | aardvark                         |
     * +-----------+----------------------------------+
     * See Also
     * Character Sets and Collations
     *
     */


    /**
     * ELT
     * Syntax
     * ELT(N, str1[, str2, str3,...])
     * Description
     * Takes a numeric argument and a series of string arguments. Returns the string that corresponds to the given numeric position. For instance, it returns str1 if N is 1, str2 if N is 2, and so on. If the numeric argument is a FLOAT, MariaDB rounds it to the nearest INTEGER. If the numeric argument is less than 1, greater than the total number of arguments, or not a number, ELT() returns NULL. It must have at least two arguments.
     * <p>
     * It is complementary to the FIELD() function.
     * <p>
     * Examples
     * SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo');
     * +------------------------------------+
     * | ELT(1, 'ej', 'Heja', 'hej', 'foo') |
     * +------------------------------------+
     * | ej                                 |
     * +------------------------------------+
     * <p>
     * SELECT ELT(4, 'ej', 'Heja', 'hej', 'foo');
     * +------------------------------------+
     * | ELT(4, 'ej', 'Heja', 'hej', 'foo') |
     * +------------------------------------+
     * | foo                                |
     * +------------------------------------+
     *
     * @param values
     * @return
     */
    public static String elt(Object... values) {
        return UnsolvedMysqlFunctionUtil.eval("elt", values).toString();
    }


    /**
     * EXPORT_SET
     * Syntax
     * EXPORT_SET(bits, on, off[, separator[, number_of_bits]])
     * Description
     * Takes a minimum of three arguments. Returns a string where each bit in the given bits argument is returned, with the string values given for on and off.
     * <p>
     * Bits are examined from right to left, (from low-order to high-order bits). Strings are added to the result from left to right, separated by a separator string (defaults as ','). You can optionally limit the number of bits the EXPORT_SET() function examines using the number_of_bits option.
     * <p>
     * If any of the arguments are set as NULL, the function returns NULL.
     * <p>
     * Examples
     * SELECT EXPORT_SET(5,'Y','N',',',4);
     * +-----------------------------+
     * | EXPORT_SET(5,'Y','N',',',4) |
     * +-----------------------------+
     * | Y,N,Y,N                     |
     * +-----------------------------+
     * <p>
     * SELECT EXPORT_SET(6,'1','0',',',10);
     * +------------------------------+
     * | EXPORT_SET(6,'1','0',',',10) |
     * +------------------------------+
     * | 0,1,1,0,0,0,0,0,0,0          |
     * +------------------------------+
     *
     * @param values
     * @return
     */
    public static String EXPORT_SET(Object... values) {
        return UnsolvedMysqlFunctionUtil.eval("EXPORT_SET", values).toString();
    }


    /**
     * EXTRACTVALUE
     * Syntax
     * EXTRACTVALUE(xml_frag, xpath_expr)
     * Contents
     * Syntax
     * Description
     * Invalid Arguments
     * Explicit text() Expressions
     * Count Matches
     * Matches
     * Examples
     * Description
     * The EXTRACTVALUE() function takes two string arguments: a fragment of XML markup and an XPath expression, (also known as a locator). It returns the text (That is, CDDATA), of the first text node which is a child of the element or elements matching the XPath expression.
     * <p>
     * In cases where a valid XPath expression does not match any text nodes in a valid XML fragment, (including the implicit /text() expression), the EXTRACTVALUE() function returns an empty string.
     * <p>
     * Invalid Arguments
     * When either the XML fragment or the XPath expression is NULL, the EXTRACTVALUE() function returns NULL. When the XML fragment is invalid, it raises a warning Code 1525:
     * <p>
     * Warning (Code 1525): Incorrect XML value: 'parse error at line 1 pos 11: unexpected END-OF-INPUT'
     * When the XPath value is invalid, it generates an Error 1105:
     * <p>
     * ERROR 1105 (HY000): XPATH syntax error: ')'
     * Explicit text() Expressions
     * This function is the equivalent of performing a match using the XPath expression after appending /text(). In other words:
     * <p>
     * SELECT
     * EXTRACTVALUE('<cases><case>example</case></cases>', '/cases/case') AS 'Base Example',
     * EXTRACTVALUE('<cases><case>example</case></cases>', '/cases/case/text()') AS 'text() Example';
     * <p>
     * +--------------+----------------+
     * | Base Example | text() Example |
     * +--------------+----------------+
     * | example      | example        |
     * +--------------+----------------+
     * Count Matches
     * When EXTRACTVALUE() returns multiple matches, it returns the content of the first child text node of each matching element, in the matched order, as a single, space-delimited string.
     * <p>
     * By design, the EXTRACTVALUE() function makes no distinction between a match on an empty element and no match at all. If you need to determine whether no matching element was found in the XML fragment or if an element was found that contained no child text nodes, use the XPath count() function.
     * <p>
     * For instance, when looking for a value that exists, but contains no child text nodes, you would get a count of the number of matching instances:
     * <p>
     * SELECT
     * EXTRACTVALUE('<cases><case/></cases>', '/cases/case') AS 'Empty Example',
     * EXTRACTVALUE('<cases><case/></cases>', 'count(/cases/case)') AS 'count() Example';
     * <p>
     * +---------------+-----------------+
     * | Empty Example | count() Example |
     * +---------------+-----------------+
     * |               |               1 |
     * +---------------+-----------------+
     * Alternatively, when looking for a value that doesn't exist, count() returns 0.
     * <p>
     * SELECT
     * EXTRACTVALUE('<cases><case/></cases>', '/cases/person') AS 'No Match Example',
     * EXTRACTVALUE('<cases><case/></cases>', 'count(/cases/person)') AS 'count() Example';
     * <p>
     * +------------------+-----------------+
     * | No Match Example | count() Example |
     * +------------------+-----------------+
     * |                  |                0|
     * +------------------+-----------------+
     * Matches
     * Important: The EXTRACTVALUE() function only returns CDDATA. It does not return tags that the element might contain or the text that these child elements contain.
     * <p>
     * SELECT EXTRACTVALUE('<cases><case>Person<email>x@example.com</email></case></cases>', '/cases') AS Case;
     * <p>
     * +--------+
     * | Case   |
     * +--------+
     * | Person |
     * +--------+
     * Note, in the above example, while the XPath expression matches to the parent <case> instance, it does not return the contained <email> tag or its content.
     * <p>
     * Examples
     * SELECT
     * ExtractValue('<a>ccc<b>ddd</b></a>', '/a')            AS val1,
     * ExtractValue('<a>ccc<b>ddd</b></a>', '/a/b')          AS val2,
     * ExtractValue('<a>ccc<b>ddd</b></a>', '//b')           AS val3,
     * ExtractValue('<a>ccc<b>ddd</b></a>', '/b')            AS val4,
     * ExtractValue('<a>ccc<b>ddd</b><b>eee</b></a>', '//b') AS val5;
     * +------+------+------+------+---------+
     * | val1 | val2 | val3 | val4 | val5    |
     * +------+------+------+------+---------+
     * | ccc  | ddd  | ddd  |      | ddd eee |
     * +------+------+------+------+---------+
     *
     * @param values
     * @return
     */
    public static String EXTRACTVALUE(Object... values) {
        return UnsolvedMysqlFunctionUtil.eval("EXTRACTVALUE", values).toString();
    }

    /**
     * FIELD
     * Syntax
     * FIELD(pattern, str1[,str2,...])
     * Description
     * Returns the index position of the string or number matching the given pattern. Returns 0 in the event that none of the arguments match the pattern. Raises an Error 1582 if not given at least two arguments.
     * <p>
     * When all arguments given to the FIELD() function are strings, they are treated as case-insensitive. When all the arguments are numbers, they are treated as numbers. Otherwise, they are treated as doubles.
     * <p>
     * If the given pattern occurs more than once, the FIELD() function only returns the index of the first instance. If the given pattern is NULL, the function returns 0, as a NULL pattern always fails to match.
     * <p>
     * This function is complementary to the ELT() function.
     * <p>
     * Examples
     * SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')
     * AS 'Field Results';
     * +---------------+
     * | Field Results |
     * +---------------+
     * |             2 |
     * +---------------+
     * <p>
     * SELECT FIELD('fo', 'Hej', 'ej', 'Heja', 'hej', 'foo')
     * AS 'Field Results';
     * +---------------+
     * | Field Results |
     * +---------------+
     * |             0 |
     * +---------------+
     * <p>
     * SELECT FIELD(1, 2, 3, 4, 5, 1) AS 'Field Results';
     * +---------------+
     * | Field Results |
     * +---------------+
     * |             5 |
     * +---------------+
     * <p>
     * SELECT FIELD(NULL, 2, 3) AS 'Field Results';
     * +---------------+
     * | Field Results |
     * +---------------+
     * |             0 |
     * +---------------+
     * <p>
     * SELECT FIELD('fail') AS 'Field Results';
     * Error 1582 (42000): Incorrect parameter count in call
     * to native function 'field'
     *
     * @param values
     * @return
     */
    public static String FIELD(Object... values) {
        return UnsolvedMysqlFunctionUtil.eval("FIELD", values).toString();
    }

    /**
     * FIND_IN_SET
     * Syntax
     * FIND_IN_SET(pattern, strlist)
     * Description
     * Returns the index position where the given pattern occurs in a string list. The first argument is the pattern you want to search for. The second argument is a string containing comma-separated variables. If the second argument is of the SET data-type, the function is optimized to use bit arithmetic.
     * <p>
     * If the pattern does not occur in the string list or if the string list is an empty string, the function returns 0. If either argument is NULL, the function returns NULL. The function does not return the correct result if the pattern contains a comma (",") character.
     * <p>
     * Examples
     * SELECT FIND_IN_SET('b','a,b,c,d') AS "Found Results";
     * +---------------+
     * | Found Results |
     * +---------------+
     * |             2 |
     * +---------------+
     *
     * @param values
     * @return
     */
    public static String FIND_IN_SET(Object... values) {
        return UnsolvedMysqlFunctionUtil.eval("FIND_IN_SET", values).toString();
    }

    /**
     * FORMAT
     * Syntax
     * FORMAT(num, decimal_position[, locale])
     * Description
     * Formats the given number for display as a string, adding separators to appropriate position and rounding the results to the given decimal position. For instance, it would format 15233.345 to 15,233.35.
     * <p>
     * If the given decimal position is 0, it rounds to return no decimal point or fractional part. You can optionally specify a locale value to format numbers to the pattern appropriate for the given region.
     * <p>
     * Examples
     * SELECT FORMAT(1234567890.09876543210, 4) AS 'Format';
     * +--------------------+
     * | Format             |
     * +--------------------+
     * | 1,234,567,890.0988 |
     * +--------------------+
     * <p>
     * SELECT FORMAT(1234567.89, 4) AS 'Format';
     * +----------------+
     * | Format         |
     * +----------------+
     * | 1,234,567.8900 |
     * +----------------+
     * <p>
     * SELECT FORMAT(1234567.89, 0) AS 'Format';
     * +-----------+
     * | Format    |
     * +-----------+
     * | 1,234,568 |
     * +-----------+
     * <p>
     * SELECT FORMAT(123456789,2,'rm_CH') AS 'Format';
     * +----------------+
     * | Format         |
     * +----------------+
     * | 123'456'789,00 |
     * +----------------+
     *
     * @param values
     * @return
     */
    public static String FORMAT(Object... values) {
        return UnsolvedMysqlFunctionUtil.eval("FORMAT", values).toString();
    }

    /**
     * FROM_BASE64
     * MariaDB starting with 10.0.5
     * The FROM_BASE64() function was introduced in MariaDB 10.0.5.
     * <p>
     * Syntax
     * FROM_BASE64(str)
     * Description
     * Decodes the given base-64 encode string, returning the result as a binary string. Returns NULL if the given string is NULL or if it's invalid.
     * <p>
     * It is the reverse of the TO_BASE64 function.
     * <p>
     * There are numerous methods to base-64 encode a string. MariaDB uses the following:
     * <p>
     * It encodes alphabet value 64 as '+'.
     * It encodes alphabet value 63 as '/'.
     * It codes output in groups of four printable characters. Each three byte of data encoded uses four characters. If the final group is incomplete, it pads the difference with the '=' character.
     * It divides long output, adding a new line very 76 characters.
     * In decoding, it recognizes and ignores newlines, carriage returns, tabs and space whitespace characters.
     * SELECT TO_BASE64('Maria') AS 'Input';
     * +-----------+
     * | Input     |
     * +-----------+
     * | TWFyaWE=  |
     * +-----------+
     * <p>
     * SELECT FROM_BASE64('TWFyaWE=') AS 'Output';
     * +--------+
     * | Output |
     * +--------+
     * | Maria  |
     * +--------+
     *
     * @param arg
     * @return
     */
    public static String FROM_BASE64(String arg) {
        return ByteString.ofBase64(arg).toString();
    }

    /**
     * HEX
     * Syntax
     * HEX(N_or_S)
     * Description
     * If N_or_S is a number, returns a string representation of the hexadecimal value of N, where N is a longlong (BIGINT) number. This is equivalent to CONV(N,10,16).
     * <p>
     * If N_or_S is a string, returns a hexadecimal string representation of N_or_S where each byte of each character in N_or_S is converted to two hexadecimal digits. If N_or_S is NULL, returns NULL. The inverse of this operation is performed by the UNHEX() function.
     * <p>
     * <p>
     * <p>
     * MariaDB starting with 10.5.0
     * HEX() with an INET6 argument returns a hexadecimal representation of the underlying 16-byte binary string.
     * <p>
     * Examples
     * SELECT HEX(255);
     * +----------+
     * | HEX(255) |
     * +----------+
     * | FF       |
     * +----------+
     * <p>
     * SELECT 0x4D617269614442;
     * +------------------+
     * | 0x4D617269614442 |
     * +------------------+
     * | MariaDB          |
     * +------------------+
     * <p>
     * SELECT HEX('MariaDB');
     * +----------------+
     * | HEX('MariaDB') |
     * +----------------+
     * | 4D617269614442 |
     * +----------------+
     * From MariaDB 10.5.0:
     * <p>
     * SELECT HEX(CAST('2001:db8::ff00:42:8329' AS INET6));
     * +----------------------------------------------+
     * | HEX(CAST('2001:db8::ff00:42:8329' AS INET6)) |
     * +----------------------------------------------+
     * | 20010DB8000000000000FF0000428329             |
     * +----------------------------------------------+
     *
     * @param n
     * @return
     */
    public static String HEX(long n) {
        return Long.toHexString(n);
    }

    public static String HEX(String s) {
        return UnsolvedMysqlFunctionUtil.eval("hex", s).toString();
    }

    /**
     * INSERT Function
     * Syntax
     * INSERT(str,pos,len,newstr)
     * Description
     * Returns the string str, with the substring beginning at position pos and len characters long replaced by the string newstr. Returns the original string if pos is not within the length of the string. Replaces the rest of the string from position pos if len is not within the length of the rest of the string. Returns NULL if any argument is NULL.
     * <p>
     * Examples
     * SELECT INSERT('Quadratic', 3, 4, 'What');
     * +-----------------------------------+
     * | INSERT('Quadratic', 3, 4, 'What') |
     * +-----------------------------------+
     * | QuWhattic                         |
     * +-----------------------------------+
     * <p>
     * SELECT INSERT('Quadratic', -1, 4, 'What');
     * +------------------------------------+
     * | INSERT('Quadratic', -1, 4, 'What') |
     * +------------------------------------+
     * | Quadratic                          |
     * +------------------------------------+
     * <p>
     * SELECT INSERT('Quadratic', 3, 100, 'What');
     * +-------------------------------------+
     * | INSERT('Quadratic', 3, 100, 'What') |
     * +-------------------------------------+
     * | QuWhat                              |
     * +-------------------------------------+
     *
     * @param s
     * @return
     */
    public static String INSERT(Object... s) {
        return UnsolvedMysqlFunctionUtil.eval("INSERT", s).toString();
    }

    /**
     * INSTR
     * Syntax
     * INSTR(str,substr)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the position of the first occurrence of substring substr in string str. This is the same as the two-argument form of LOCATE(), except that the order of the arguments is reversed.
     * <p>
     * INSTR() performs a case-insensitive search.
     * <p>
     * If any argument is NULL, returns NULL.
     * <p>
     * Examples
     * SELECT INSTR('foobarbar', 'bar');
     * +---------------------------+
     * | INSTR('foobarbar', 'bar') |
     * +---------------------------+
     * |                         4 |
     * +---------------------------+
     * <p>
     * SELECT INSTR('My', 'Maria');
     * +----------------------+
     * | INSTR('My', 'Maria') |
     * +----------------------+
     * |                    0 |
     * +----------------------+
     * See Also
     * INSTR() ; Returns the position of a string withing a string
     * SUBSTRING_INDEX() ; Returns the substring from string before count occurrences of a delimiter
     *
     * @param s
     * @return
     */
    public static String INSTR(Object... s) {
        return UnsolvedMysqlFunctionUtil.eval("INSTR", s).toString();
    }

    /**
     * LCASE
     * Syntax
     * LCASE(str)
     * Description
     * LCASE() is a synonym for LOWER().
     *
     * @param s
     * @return
     */
    public static String LCASE(String s) {
        return LOWER(s);
    }

    /**
     * LOWER
     * Syntax
     * LOWER(str)
     * Contents
     * Syntax
     * Description
     * Examples
     * Description
     * Returns the string str with all characters changed to lowercase according to the current character set mapping. The default is latin1 (cp1252 West European).
     * <p>
     * Examples
     * SELECT LOWER('QUADRATICALLY');
     * +------------------------+
     * | LOWER('QUADRATICALLY') |
     * +------------------------+
     * | quadratically          |
     * +------------------------+
     * LOWER() (and UPPER()) are ineffective when applied to binary strings (BINARY, VARBINARY, BLOB). To perform lettercase conversion, CONVERT the string to a non-binary string:
     * <p>
     * SET @str = BINARY 'North Carolina';
     * <p>
     * SELECT LOWER(@str), LOWER(CONVERT(@str USING latin1));
     * +----------------+-----------------------------------+
     * | LOWER(@str)    | LOWER(CONVERT(@str USING latin1)) |
     * +----------------+-----------------------------------+
     * | North Carolina | north carolina                    |
     * +----------------+-----------------------------------+
     *
     * @param s
     * @return
     */
    public static String LOWER(String s) {
        if (s == null) {
            return s;
        }
        return s.toLowerCase();
    }

    /**
     * LEFT
     * Syntax
     * LEFT(str,len)
     * Description
     * Returns the leftmost len characters from the string str, or NULL if any argument is NULL.
     * <p>
     * Examples
     * SELECT LEFT('MariaDB', 5);
     * +--------------------+
     * | LEFT('MariaDB', 5) |
     * +--------------------+
     * | Maria              |
     * +--------------------+
     *
     * @param s
     * @param len
     * @return
     */
    public static String LEFT(String s, int len) {
        if (s == null) {
            return s;
        }
        return s.substring(0, len);
    }


    /**
     * LENGTH
     * Syntax
     * LENGTH(str)
     * Contents
     * Syntax
     * Description
     * Oracle Mode
     * Examples
     * See Also
     * Description
     * Returns the length of the string str, measured in bytes. A multi-byte character counts as multiple bytes. This means that for a string containing five two-byte characters, LENGTH() returns 10, whereas CHAR_LENGTH() returns 5.
     * <p>
     * If str is not a string value, it is converted into a string. If str is NULL, the function returns NULL.
     * <p>
     * Until MariaDB 10.3.1, returns MYSQL_TYPE_LONGLONG, or bigint(10), in all cases. From MariaDB 10.3.1, returns MYSQL_TYPE_LONG, or int(10), when the result would fit within 32-bits.
     * <p>
     * Oracle Mode
     * MariaDB starting with 10.3
     * When running Oracle mode from MariaDB 10.3, LENGTH() is a synonym for CHAR_LENGTH().
     * <p>
     * Examples
     * SELECT LENGTH('MariaDB');
     * +-------------------+
     * | LENGTH('MariaDB') |
     * +-------------------+
     * |                 7 |
     * +-------------------+
     * <p>
     * SELECT LENGTH('π');
     * +--------------+
     * | LENGTH('π')  |
     * +--------------+
     * |            2 |
     * +--------------+
     * In Oracle mode from MariaDB 10.3:
     * <p>
     * SELECT LENGTH('π');
     * +--------------+
     * | LENGTH('π')  |
     * +--------------+
     * |            1 |
     * +--------------+
     *
     * @param s
     * @return
     */
    public static Integer LENGTH(String s) {
        if (s == null) {
            return null;
        }
        return s.length();
    }


    /**
     * LENGTHB
     * MariaDB starting with 10.3.1
     * Introduced in MariaDB 10.3.1 as part of the Oracle compatibility enhancements.
     * <p>
     * Syntax
     * LENGTHB(str)
     * Description
     * LENGTHB() is a synonym for LENGTH().
     *
     * @param s
     * @return
     */
    public static Integer LENGTHB(String s) {
        return LENGTH(s);
    }

    /**
     * LIKE
     * Syntax
     * expr LIKE pat [ESCAPE 'escape_char']
     * expr NOT LIKE pat [ESCAPE 'escape_char']
     * Contents
     * Syntax
     * Description
     * Examples
     * Optimizing LIKE
     * See Also
     * Description
     * Tests whether expr matches the pattern pat. Returns either 1 (TRUE) or 0 (FALSE). Both expr and pat may be any valid expression and are evaluated to strings. Patterns may use the following wildcard characters:
     *
     * % matches any number of characters, including zero.
     * _ matches any single character.
     * Use NOT LIKE to test if a string does not match a pattern. This is equivalent to using the NOT operator on the entire LIKE expression.
     *
     * If either the expression or the pattern is NULL, the result is NULL.
     *
     * LIKE performs case-insensitive substring matches if the collation for the expression and pattern is case-insensitive. For case-sensitive matches, declare either argument to use a binary collation using COLLATE, or coerce either of them to a BINARY string using CAST. Use SHOW COLLATION to get a list of available collations. Collations ending in _bin are case-sensitive.
     *
     * Numeric arguments are coerced to binary strings.
     *
     * The _ wildcard matches a single character, not byte. It will only match a multi-byte character if it is valid in the expression's character set. For example, _ will match _utf8"€", but it will not match _latin1"€" because the Euro sign is not a valid latin1 character. If necessary, use CONVERT to use the expression in a different character set.
     *
     * If you need to match the characters _ or %, you must escape them. By default, you can prefix the wildcard characters the backslash character \ to escape them. The backslash is used both to encode special characters like newlines when a string is parsed as well as to escape wildcards in a pattern after parsing. Thus, to match an actual backslash, you sometimes need to double-escape it as "\\\\".
     *
     * To avoid difficulties with the backslash character, you can change the wildcard escape character using ESCAPE in a LIKE expression. The argument to ESCAPE must be a single-character string.
     *
     * Examples
     * Select the days that begin with "T":
     *
     * CREATE TABLE t1 (d VARCHAR(16));
     * INSERT INTO t1 VALUES ("Monday"), ("Tuesday"), ("Wednesday"), ("Thursday"), ("Friday"), ("Saturday"), ("Sunday");
     * SELECT * FROM t1 WHERE d LIKE "T%";
     * SELECT * FROM t1 WHERE d LIKE "T%";
     * +----------+
     * | d        |
     * +----------+
     * | Tuesday  |
     * | Thursday |
     * +----------+
     * Select the days that contain the substring "es":
     *
     * SELECT * FROM t1 WHERE d LIKE "%es%";
     * SELECT * FROM t1 WHERE d LIKE "%es%";
     * +-----------+
     * | d         |
     * +-----------+
     * | Tuesday   |
     * | Wednesday |
     * +-----------+
     * Select the six-character day names:
     *
     * SELECT * FROM t1 WHERE d like "___day";
     * SELECT * FROM t1 WHERE d like "___day";
     * +---------+
     * | d       |
     * +---------+
     * | Monday  |
     * | Friday  |
     * | Sunday  |
     * +---------+
     * With the default collations, LIKE is case-insensitive:
     *
     * SELECT * FROM t1 where d like "t%";
     * SELECT * FROM t1 where d like "t%";
     * +----------+
     * | d        |
     * +----------+
     * | Tuesday  |
     * | Thursday |
     * +----------+
     * Use COLLATE to specify a binary collation, forcing case-sensitive matches:
     *
     * SELECT * FROM t1 WHERE d like "t%" COLLATE latin1_bin;
     * SELECT * FROM t1 WHERE d like "t%" COLLATE latin1_bin;
     * Empty set (0.00 sec)
     * You can include functions and operators in the expression to match. Select dates based on their day name:
     *
     * CREATE TABLE t2 (d DATETIME);
     * INSERT INTO t2 VALUES
     *     ("2007-01-30 21:31:07"),
     *     ("1983-10-15 06:42:51"),
     *     ("2011-04-21 12:34:56"),
     *     ("2011-10-30 06:31:41"),
     *     ("2011-01-30 14:03:25"),
     *     ("2004-10-07 11:19:34");
     * SELECT * FROM t2 WHERE DAYNAME(d) LIKE "T%";
     * SELECT * FROM t2 WHERE DAYNAME(d) LIKE "T%";
     * +------------------+
     * | d                |
     * +------------------+
     * | 2007-01-30 21:31 |
     * | 2011-04-21 12:34 |
     * | 2004-10-07 11:19 |
     * +------------------+
     * 3 rows in set, 7 warnings (0.00 sec)
     * Optimizing LIKE
     * MariaDB can use indexes for LIKE on string columns in the case where the LIKE doesn't start with % or _.
     * Starting from MariaDB 10.0, one can set the optimizer_use_condition_selectivity variable to 5. If this is done, then the optimizer will read optimizer_selectivity_sampling_limit rows to calculate the selectivity of the LIKE expression before starting to calculate the query plan. This can help speed up some LIKE queries by providing the optimizer with more information about your data.
     * See Also
     * For searches on text columns, with results sorted by relevance, see full-text indexes.
     * For more complex searches and operations on strings, you can use regular expressions, which were enhanced in MariaDB 10 (see PCRE Regular Expressions).
     *
     */

    /**
     * LOAD_FILE
     * Syntax
     * LOAD_FILE(file_name)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Reads the file and returns the file contents as a string. To use this function, the file must be located on the server host, you must specify the full path name to the file, and you must have the FILE privilege. The file must be readable by all and it must be less than the size, in bytes, of the max_allowed_packet system variable. If the secure_file_priv system variable is set to a non-empty directory name, the file to be loaded must be located in that directory.
     *
     * If the file does not exist or cannot be read because one of the preceding conditions is not satisfied, the function returns NULL.
     *
     * Since MariaDB 5.1, the character_set_filesystem system variable has controlled interpretation of file names that are given as literal strings.
     *
     * Statements using the LOAD_FILE() function are not safe for statement based replication. This is because the slave will execute the LOAD_FILE() command itself. If the file doesn't exist on the slave, the function will return NULL.
     *
     * Examples
     * UPDATE t SET blob_col=LOAD_FILE('/tmp/picture') WHERE id=1;
     *
     */


    /**
     * LOCATE
     * Syntax
     * LOCATE(substr,str), LOCATE(substr,str,pos)
     * Description
     * The first syntax returns the position of the first occurrence of substring substr in string str. The second syntax returns the position of the first occurrence of substring substr in string str, starting at position pos. Returns 0 if substr is not in str.
     * <p>
     * LOCATE() performs a case-insensitive search.
     * <p>
     * If any argument is NULL, returns NULL.
     * <p>
     * INSTR() is a synonym of LOCATE() without the third argument.
     * <p>
     * Examples
     * SELECT LOCATE('bar', 'foobarbar');
     * +----------------------------+
     * | LOCATE('bar', 'foobarbar') |
     * +----------------------------+
     * |                          4 |
     * +----------------------------+
     * <p>
     * SELECT LOCATE('My', 'Maria');
     * +-----------------------+
     * | LOCATE('My', 'Maria') |
     * +-----------------------+
     * |                     0 |
     * +-----------------------+
     * <p>
     * SELECT LOCATE('bar', 'foobarbar', 5);
     * +-------------------------------+
     * | LOCATE('bar', 'foobarbar', 5) |
     * +-------------------------------+
     * |                             7 |
     * +-------------------------------+
     *
     * @param args
     * @return
     */
    public static Integer LOCATE(Object... args) {
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("LOCATE", args).toString());
    }

    /**
     * LPAD
     * Syntax
     * LPAD(str, len [,padstr])
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the string str, left-padded with the string padstr to a length of len characters. If str is longer than len, the return value is shortened to len characters. If padstr is omitted, the LPAD function pads spaces.
     * <p>
     * Prior to MariaDB 10.3.1, the padstr parameter was mandatory.
     * <p>
     * Returns NULL if given a NULL argument. If the result is empty (zero length), returns either an empty string or, from MariaDB 10.3.6 with SQL_MODE=Oracle, NULL.
     * <p>
     * The Oracle mode version of the function can be accessed outside of Oracle mode by using LPAD_ORACLE as the function name.
     * <p>
     * Examples
     * SELECT LPAD('hello',10,'.');
     * +----------------------+
     * | LPAD('hello',10,'.') |
     * +----------------------+
     * | .....hello           |
     * +----------------------+
     * <p>
     * SELECT LPAD('hello',2,'.');
     * +---------------------+
     * | LPAD('hello',2,'.') |
     * +---------------------+
     * | he                  |
     * +---------------------+
     * From MariaDB 10.3.1, with the pad string defaulting to space.
     * <p>
     * SELECT LPAD('hello',10);
     * +------------------+
     * | LPAD('hello',10) |
     * +------------------+
     * |      hello       |
     * +------------------+
     * Oracle mode version from MariaDB 10.3.6:
     * <p>
     * SELECT LPAD('',0),LPAD_ORACLE('',0);
     * +------------+-------------------+
     * | LPAD('',0) | LPAD_ORACLE('',0) |
     * +------------+-------------------+
     * |            | NULL              |
     * +------------+-------------------+
     * See Also
     * RPAD - Right-padding instead of left-padding.
     *
     * @param args
     * @return
     */
    public static String LPAD(Object... args) {
        Object lpad = UnsolvedMysqlFunctionUtil.eval("LPAD", args);
        if (lpad == null) {
            return null;
        }
        return lpad.toString();
    }

    /**
     * LTRIM
     * Syntax
     * LTRIM(str)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the string str with leading space characters removed.
     * <p>
     * Returns NULL if given a NULL argument. If the result is empty, returns either an empty string, or, from MariaDB 10.3.6 with SQL_MODE=Oracle, NULL.
     * <p>
     * The Oracle mode version of the function can be accessed outside of Oracle mode by using LTRIM_ORACLE as the function name.
     * <p>
     * Examples
     * SELECT QUOTE(LTRIM('   MariaDB   '));
     * +-------------------------------+
     * | QUOTE(LTRIM('   MariaDB   ')) |
     * +-------------------------------+
     * | 'MariaDB   '                  |
     * +-------------------------------+
     * Oracle mode version from MariaDB 10.3.6:
     * <p>
     * SELECT LTRIM(''),LTRIM_ORACLE('');
     * +-----------+------------------+
     * | LTRIM('') | LTRIM_ORACLE('') |
     * +-----------+------------------+
     * |           | NULL             |
     * +-----------+------------------+
     * See Also
     * RTRIM - trailing spaces removed
     * TRIM - removes all given prefixes or suffixes
     *
     * @param arg
     * @return
     */
    public static String LTRIM(String arg) {
        if (arg == null) {
            return null;
        }
        return StringUtils.ltrim(arg);
    }


    /**
     * MAKE_SET
     * Syntax
     * MAKE_SET(bits,str1,str2,...)
     * Description
     * Returns a set value (a string containing substrings separated by "," characters) consisting of the strings that have the corresponding bit in bits set. str1 corresponds to bit 0, str2 to bit 1, and so on. NULL values in str1, str2, ... are not appended to the result.
     * <p>
     * Examples
     * SELECT MAKE_SET(1,'a','b','c');
     * +-------------------------+
     * | MAKE_SET(1,'a','b','c') |
     * +-------------------------+
     * | a                       |
     * +-------------------------+
     * <p>
     * SELECT MAKE_SET(1 | 4,'hello','nice','world');
     * +----------------------------------------+
     * | MAKE_SET(1 | 4,'hello','nice','world') |
     * +----------------------------------------+
     * | hello,world                            |
     * +----------------------------------------+
     * <p>
     * SELECT MAKE_SET(1 | 4,'hello','nice',NULL,'world');
     * +---------------------------------------------+
     * | MAKE_SET(1 | 4,'hello','nice',NULL,'world') |
     * +---------------------------------------------+
     * | hello                                       |
     * +---------------------------------------------+
     * <p>
     * SELECT QUOTE(MAKE_SET(0,'a','b','c'));
     * +--------------------------------+
     * | QUOTE(MAKE_SET(0,'a','b','c')) |
     * +--------------------------------+
     * | ''                             |
     * +--------------------------------+
     *
     * @param args
     * @return
     */
    public static String MAKE_SET(Object... args) {
        Object make_set = UnsolvedMysqlFunctionUtil.eval("MAKE_SET", args);
        if (make_set == null) {
            return null;
        } else {
            return make_set.toString();
        }
    }

    public static Integer REGEXP(String expr, String pat) {
        if (expr == null || pat == null) {
            return null;
        }
        return expr.matches(".*" + pat + ".*") ? 1 : 0;
    }
    /***
     * MATCH AGAINST
     * Syntax
     * MATCH (col1,col2,...) AGAINST (expr [search_modifier])
     * Description
     * A special construct used to perform a fulltext search on a fulltext index.
     *
     * See Fulltext Index Overview for a full description, and Full-text Indexes for more articles on the topic.
     *
     * Examples
     * CREATE TABLE ft_myisam(copy TEXT,FULLTEXT(copy)) ENGINE=MyISAM;
     *
     * INSERT INTO ft_myisam(copy) VALUES ('Once upon a time'), ('There was a wicked witch'),
     *  ('Who ate everybody up');
     *
     * SELECT * FROM ft_myisam WHERE MATCH(copy) AGAINST('wicked');
     * +--------------------------+
     * | copy                     |
     * +--------------------------+
     * | There was a wicked witch |
     * +--------------------------+
     * SELECT id, body, MATCH (title,body) AGAINST
     *     ('Security implications of running MySQL as root'
     *     IN NATURAL LANGUAGE MODE) AS score
     *     FROM articles WHERE MATCH (title,body) AGAINST
     *     ('Security implications of running MySQL as root'
     *     IN NATURAL LANGUAGE MODE);
     * +----+-------------------------------------+-----------------+
     * | id | body                                | score           |
     * +----+-------------------------------------+-----------------+
     * |  4 | 1. Never run mysqld as root. 2. ... | 1.5219271183014 |
     * |  6 | When configured properly, MySQL ... | 1.3114095926285 |
     * +----+-------------------------------------+-----------------+
     *
     *
     */

    /**
     * Full-Text Index Stopwords
     */

    /**
     * MID
     * Syntax
     * MID(str,pos,len)
     * Description
     * MID(str,pos,len) is a synonym for SUBSTRING(str,pos,len).
     * <p>
     * Examples
     * SELECT MID('abcd',4,1);
     * +-----------------+
     * | MID('abcd',4,1) |
     * +-----------------+
     * | d               |
     * +-----------------+
     * <p>
     * SELECT MID('abcd',2,2);
     * +-----------------+
     * | MID('abcd',2,2) |
     * +-----------------+
     * | bc              |
     * +-----------------+
     * A negative starting position:
     * <p>
     * SELECT MID('abcd',-2,4);
     * +------------------+
     * | MID('abcd',-2,4) |
     * +------------------+
     * | cd               |
     * +------------------+
     *
     * @param str
     * @param pos
     * @param len
     * @return
     */
    public static String mid(String str, int pos, int len) {
        return SUBSTRING(str, pos, len + pos);
    }

    public static String SUBSTRING(String str, int pos, int len) {
        return str.substring(pos, len + pos);
    }

    /**
     * NOT LIKE
     * Syntax
     * expr NOT LIKE pat [ESCAPE 'escape_char']
     * Description
     * This is the same as NOT (expr LIKE pat [ESCAPE 'escape_char']).
     *
     */

    /**
     * NOT REGEXP
     * Syntax
     * expr NOT REGEXP pat, expr NOT RLIKE pat
     * Description
     * This is the same as NOT (expr REGEXP pat).
     */

    public static Integer OCTET_LENGTH(String str) {
        if (str == null) {
            return null;
        }
        return str.length();
    }

    /**
     * ORD
     * Syntax
     * ORD(str)
     * Description
     * If the leftmost character of the string str is a multi-byte character, returns the code for that character, calculated from the numeric values of its constituent bytes using this formula:
     * <p>
     * (1st byte code)
     * + (2nd byte code x 256)
     * + (3rd byte code x 256 x 256) ...
     * If the leftmost character is not a multi-byte character, ORD() returns the same value as the ASCII() function.
     * <p>
     * Examples
     * SELECT ORD('2');
     * +----------+
     * | ORD('2') |
     * +----------+
     * |       50 |
     * +----------+
     * See Also
     * ASCII() - Return ASCII value of first character
     * CHAR() - Create a character from an integer value
     *
     * @param str
     * @return
     */
    public static Integer ORD(String str) {
        if (str == null) {
            return null;
        }
        return Integer.parseInt(
                UnsolvedMysqlFunctionUtil.eval("ord", str).toString());
    }

    /**
     * POSITION
     * Syntax
     * POSITION(substr IN str)
     * Description
     * POSITION(substr IN str) is a synonym for LOCATE(substr,str).
     *
     * It's part of ODBC 3.0.
     * 基于语法树修改支持
     */

    /**
     * QUOTE
     * Syntax
     * QUOTE(str)
     * Description
     * Quotes a string to produce a result that can be used as a properly escaped data value in an SQL statement. The string is returned enclosed by single quotes and with each instance of single quote ("'"), backslash ("\"), ASCII NUL, and Control-Z preceded by a backslash. If the argument is NULL, the return value is the word "NULL" without enclosing single quotes.
     * <p>
     * Examples
     * SELECT QUOTE("Don't!");
     * +-----------------+
     * | QUOTE("Don't!") |
     * +-----------------+
     * | 'Don\'t!'       |
     * +-----------------+
     * <p>
     * SELECT QUOTE(NULL);
     * +-------------+
     * | QUOTE(NULL) |
     * +-------------+
     * | NULL        |
     * +-------------+
     */

    public static String QUOTE(String str) {
        if (str == null) {
            return null;
        }
        return UnsolvedMysqlFunctionUtil.eval("QUOTE", str).toString();
    }

    /**
     * REPEAT Function
     * Syntax
     * REPEAT(str,count)
     * Description
     * Returns a string consisting of the string str repeated count times. If count is less than 1, returns an empty string. Returns NULL if str or count are NULL.
     * <p>
     * Examples
     * SELECT QUOTE(REPEAT('MariaDB ',4));
     * +------------------------------------+
     * | QUOTE(REPEAT('MariaDB ',4))        |
     * +------------------------------------+
     * | 'MariaDB MariaDB MariaDB MariaDB ' |
     * +------------------------------------+
     */
    public static String REPEAT(String str, int count) {
        if (str == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    /**
     * REPLACE Function
     * Syntax
     * REPLACE(str,from_str,to_str)
     * Description
     * Returns the string str with all occurrences of the string from_str replaced by the string to_str. REPLACE() performs a case-sensitive match when searching for from_str.
     * <p>
     * Examples
     * SELECT REPLACE('www.mariadb.org', 'w', 'Ww');
     * +---------------------------------------+
     * | REPLACE('www.mariadb.org', 'w', 'Ww') |
     * +---------------------------------------+
     * | WwWwWw.mariadb.org                    |
     * +---------------------------------------+
     */
    public static String REPLACE(String str, String from_str, String to_str) {
        if (str == null || from_str == null || to_str == null) {
            return null;
        }
        return str.replace(from_str, to_str);
    }

    /**
     * REVERSE
     * Syntax
     * REVERSE(str)
     * Description
     * Returns the string str with the order of the characters reversed.
     * <p>
     * Examples
     * SELECT REVERSE('desserts');
     * +---------------------+
     * | REVERSE('desserts') |
     * +---------------------+
     * | stressed            |
     * +---------------------+
     *
     * @param str
     * @return
     */
    public static String REVERSE(String str) {
        if (str == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        int length = str.length();
        for (int i = length - 1; i >= 0; i--) {
            sb.append(str.charAt(i));
        }
        return sb.toString();
    }

    /**
     * RIGHT
     * Syntax
     * RIGHT(str,len)
     * Description
     * Returns the rightmost len characters from the string str, or NULL if any argument is NULL.
     * <p>
     * Examples
     * SELECT RIGHT('MariaDB', 2);
     * +---------------------+
     * | RIGHT('MariaDB', 2) |
     * +---------------------+
     * | DB                  |
     * +---------------------+
     *
     * @param str
     * @return
     */
    public static String RIGHT(String str, int len) {
        if (str == null) {
            return null;
        }
        return str.substring(str.length() - len);
    }

    /**
     * RPAD
     * Syntax
     * RPAD(str, len [, padstr])
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the string str, right-padded with the string padstr to a length of len characters. If str is longer than len, the return value is shortened to len characters. If padstr is omitted, the RPAD function pads spaces.
     * <p>
     * Prior to MariaDB 10.3.1, the padstr parameter was mandatory.
     * <p>
     * Returns NULL if given a NULL argument. If the result is empty (a length of zero), returns either an empty string, or, from MariaDB 10.3.6 with SQL_MODE=Oracle, NULL.
     * <p>
     * The Oracle mode version of the function can be accessed outside of Oracle mode by using RPAD_ORACLE as the function name.
     * <p>
     * Examples
     * SELECT RPAD('hello',10,'.');
     * +----------------------+
     * | RPAD('hello',10,'.') |
     * +----------------------+
     * | hello.....           |
     * +----------------------+
     * <p>
     * SELECT RPAD('hello',2,'.');
     * +---------------------+
     * | RPAD('hello',2,'.') |
     * +---------------------+
     * | he                  |
     * +---------------------+
     * From MariaDB 10.3.1, with the pad string defaulting to space.
     * <p>
     * SELECT RPAD('hello',30);
     * +--------------------------------+
     * | RPAD('hello',30)               |
     * +--------------------------------+
     * | hello                          |
     * +--------------------------------+
     * Oracle mode version from MariaDB 10.3.6:
     * <p>
     * SELECT RPAD('',0),RPAD_ORACLE('',0);
     * +------------+-------------------+
     * | RPAD('',0) | RPAD_ORACLE('',0) |
     * +------------+-------------------+
     * |            | NULL              |
     * +------------+-------------------+
     *
     * @param args
     * @return
     */
    public static String RPAD(Object... args) {
        Object rpad = UnsolvedMysqlFunctionUtil.eval("RPAD", args);
        if (rpad == null) {
            return null;
        }
        return rpad.toString();
    }

    /**
     * RTRIM
     * Syntax
     * RTRIM(str)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the string str with trailing space characters removed.
     * <p>
     * Returns NULL if given a NULL argument. If the result is empty, returns either an empty string, or, from MariaDB 10.3.6 with SQL_MODE=Oracle, NULL.
     * <p>
     * The Oracle mode version of the function can be accessed outside of Oracle mode by using RTRIM_ORACLE as the function name.
     * <p>
     * Examples
     * SELECT QUOTE(RTRIM('MariaDB    '));
     * +-----------------------------+
     * | QUOTE(RTRIM('MariaDB    ')) |
     * +-----------------------------+
     * | 'MariaDB'                   |
     * +-----------------------------+
     * Oracle mode version from MariaDB 10.3.6:
     * <p>
     * SELECT RTRIM(''),RTRIM_ORACLE('');
     * +-----------+------------------+
     * | RTRIM('') | RTRIM_ORACLE('') |
     * +-----------+------------------+
     * |           | NULL             |
     * +-----------+------------------+
     *
     * @param arg
     * @return
     */
    public static String RTRIM(String arg) {
        return StringUtils.rtrim(arg);
    }

    /**
     * SOUNDEX
     * Syntax
     * SOUNDEX(str)
     * Description
     * Returns a soundex string from str. Two strings that sound almost the same should have identical soundex strings. A standard soundex string is four characters long, but the SOUNDEX() function returns an arbitrarily long string. You can use SUBSTRING() on the result to get a standard soundex string. All non-alphabetic characters in str are ignored. All international alphabetic characters outside the A-Z range are treated as vowels.
     * <p>
     * Important: When using SOUNDEX(), you should be aware of the following limitations:
     * <p>
     * This function, as currently implemented, is intended to work well with strings that are in the English language only. Strings in other languages may not produce reliable results.
     * Examples
     * SOUNDEX('Hello');
     * +------------------+
     * | SOUNDEX('Hello') |
     * +------------------+
     * | H400             |
     * +------------------+
     * SELECT SOUNDEX('MariaDB');
     * +--------------------+
     * | SOUNDEX('MariaDB') |
     * +--------------------+
     * | M631               |
     * +--------------------+
     * SELECT SOUNDEX('Knowledgebase');
     * +--------------------------+
     * | SOUNDEX('Knowledgebase') |
     * +--------------------------+
     * | K543212                  |
     * +--------------------------+
     * SELECT givenname, surname FROM users WHERE SOUNDEX(givenname) = SOUNDEX("robert");
     * +-----------+---------+
     * | givenname | surname |
     * +-----------+---------+
     * | Roberto   | Castro  |
     * +-----------+---------+
     *
     * @param arg
     * @return
     */
    public static String SOUNDEX(String arg) {
        return OneParamFunctions.soundex(arg);
    }

    /**
     * SOUNDS LIKE
     * Syntax
     * expr1 SOUNDS LIKE expr2
     * Description
     * This is the same as SOUNDEX(expr1) = SOUNDEX(expr2).
     *
     * Example
     * SELECT givenname, surname FROM users WHERE givenname SOUNDS LIKE "robert";
     * +-----------+---------+
     * | givenname | surname |
     * +-----------+---------+
     * | Roberto   | Castro  |
     * +-----------+---------+
     */


    /**
     * SPACE
     * Syntax
     * SPACE(N)
     * Description
     * Returns a string consisting of N space characters. If N is NULL, returns NULL.
     * <p>
     * Examples
     * SELECT QUOTE(SPACE(6));
     * +-----------------+
     * | QUOTE(SPACE(6)) |
     * +-----------------+
     * | '      '        |
     * +-----------------+
     *
     * @param n
     * @return
     */
    public static String SPACE(Integer n) {
        if (n == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }


    /**
     * STRCMP
     * Syntax
     * STRCMP(expr1,expr2)
     * Description
     * STRCMP() returns 0 if the strings are the same, -1 if the first argument is smaller than the second according to the current sort order, and 1 otherwise.
     * <p>
     * Examples
     * SELECT STRCMP('text', 'text2');
     * +-------------------------+
     * | STRCMP('text', 'text2') |
     * +-------------------------+
     * |                      -1 |
     * +-------------------------+
     * <p>
     * SELECT STRCMP('text2', 'text');
     * +-------------------------+
     * | STRCMP('text2', 'text') |
     * +-------------------------+
     * |                       1 |
     * +-------------------------+
     * <p>
     * SELECT STRCMP('text', 'text');
     * +------------------------+
     * | STRCMP('text', 'text') |
     * +------------------------+
     * |                      0 |
     * +------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static Integer STRCMP(String arg0, String arg1) {
        if (arg0 == null || arg1 == null) {
            return null;
        }

        return arg0.compareTo(arg1);
    }

    /**
     * SUBSTR
     * Description
     * SUBSTR() is a synonym for SUBSTRING().
     */
    public static String SUBSTR(String arg0, int pos, int length) {
        return SUBSTRING(arg0, pos, length);
    }

    /**
     * SUBSTRING_INDEX
     * Syntax
     * SUBSTRING_INDEX(str,delim,count)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the substring from string str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned. SUBSTRING_INDEX() performs a case-sensitive match when searching for delim.
     * <p>
     * If any argument is NULL, returns NULL.
     * <p>
     * Examples
     * SELECT SUBSTRING_INDEX('www.mariadb.org', '.', 2);
     * +--------------------------------------------+
     * | SUBSTRING_INDEX('www.mariadb.org', '.', 2) |
     * +--------------------------------------------+
     * | www.mariadb                                |
     * +--------------------------------------------+
     * <p>
     * SELECT SUBSTRING_INDEX('www.mariadb.org', '.', -2);
     * +---------------------------------------------+
     * | SUBSTRING_INDEX('www.mariadb.org', '.', -2) |
     * +---------------------------------------------+
     * | mariadb.org                                 |
     * +---------------------------------------------+
     *
     * @param arg0
     * @param delim
     * @param count
     * @return
     */
    public static String SUBSTRING_INDEX(String arg0, String delim, int count) {
        Object s = UnsolvedMysqlFunctionUtil.eval("SUBSTRING_INDEX", arg0, delim, count);
        if (s == null) {
            return null;
        } else {
            return s.toString();
        }
    }

    /**
     * TO_BASE64
     * MariaDB starting with 10.0.5
     * The TO_BASE64() function was introduced in MariaDB 10.0.5.
     * <p>
     * Syntax
     * TO_BASE64(str)
     * Description
     * Converts the string argument str to its base-64 encoded form, returning the result as a character string in the connection character set and collation.
     * <p>
     * The argument str will be converted to string first if it is not a string. A NULL argument will return a NULL result.
     * <p>
     * The reverse function, FROM_BASE64(), decodes an encoded base-64 string.
     * <p>
     * There are a numerous different methods to base-64 encode a string. The following are used by MariaDB and MySQL:
     * <p>
     * Alphabet value 64 is encoded as '+'.
     * Alphabet value 63 is encoded as '/'.
     * Encoding output is made up of groups of four printable characters, with each three bytes of data encoded using four characters. If the final group is not complete, it is padded with '=' characters to make up a length of four.
     * To divide long output, a newline is added after every 76 characters.
     * Decoding will recognize and ignore newlines, carriage returns, tabs, and spaces.
     * Examples
     * SELECT TO_BASE64('Maria');
     * +--------------------+
     * | TO_BASE64('Maria') |
     * +--------------------+
     * | TWFyaWE=           |
     * +--------------------+
     *
     * @param arg0
     * @return
     */
    public static String TO_BASE64(String arg0) {
        if (arg0 == null) {
            return null;
        }
        return new String(Base64.altBase64ToByteArray(arg0));
    }

    /**
     * TRIM
     * Syntax
     * TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the string str with all remstr prefixes or suffixes removed. If none of the specifiers BOTH, LEADING, or TRAILING is given, BOTH is assumed. remstr is optional and, if not specified, spaces are removed.
     * <p>
     * Returns NULL if given a NULL argument. If the result is empty, returns either an empty string, or, from MariaDB 10.3.6 with SQL_MODE=Oracle, NULL.
     * <p>
     * The Oracle mode version of the function can be accessed outside of Oracle mode by using TRIM_ORACLE as the function name.
     * <p>
     * Examples
     * SELECT TRIM('  bar   ')\G
     * *************************** 1. row ***************************
     * TRIM('  bar   '): bar
     * <p>
     * SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx')\G
     * *************************** 1. row ***************************
     * TRIM(LEADING 'x' FROM 'xxxbarxxx'): barxxx
     * <p>
     * SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx')\G
     * *************************** 1. row ***************************
     * TRIM(BOTH 'x' FROM 'xxxbarxxx'): bar
     * <p>
     * SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz')\G
     * *************************** 1. row ***************************
     * TRIM(TRAILING 'xyz' FROM 'barxxyz'): barx
     * Oracle mode version from MariaDB 10.3.6:
     * <p>
     * SELECT TRIM(''),TRIM_ORACLE('');
     * +----------+-----------------+
     * | TRIM('') | TRIM_ORACLE('') |
     * +----------+-----------------+
     * |          | NULL            |
     * +----------+-----------------+
     *
     * @param arg0
     * @return
     */
    public static String trim(String arg0) {
        if (arg0 == null) {
            return null;
        }
        return arg0.trim();
    }

    /**
     * UCASE
     * Syntax
     * UCASE(str)
     * Description
     * UCASE() is a synonym for UPPER().
     *
     * @param arg0
     * @return
     */
    public static String UCASE(String arg0) {
        return UPPER(arg0);
    }

    /**
     * UPPER
     * Syntax
     * UPPER(str)
     * Description
     * Returns the string str with all characters changed to uppercase according to the current character set mapping. The default is latin1 (cp1252 West European).
     * <p>
     * SELECT UPPER(surname), givenname FROM users ORDER BY surname;
     * +----------------+------------+
     * | UPPER(surname) | givenname  |
     * +----------------+------------+
     * | ABEL           | Jacinto    |
     * | CASTRO         | Robert     |
     * | COSTA          | Phestos    |
     * | MOSCHELLA      | Hippolytos |
     * +----------------+------------+
     * UPPER() is ineffective when applied to binary strings (BINARY, VARBINARY, BLOB). The description of LOWER() shows how to perform lettercase conversion of binary strings.
     *
     * @param arg0
     * @return
     */
    public static String UPPER(String arg0) {
        if (arg0 == null) {
            return null;
        }
        return arg0.toUpperCase();
    }


    /**
     * UNHEX
     * Syntax
     * UNHEX(str)
     * Description
     * Performs the inverse operation of HEX(str). That is, it interprets each pair of hexadecimal digits in the argument as a number and converts it to the character represented by the number. The resulting characters are returned as a binary string.
     * <p>
     * If str is NULL, UNHEX() returns NULL.
     * <p>
     * Examples
     * SELECT HEX('MariaDB');
     * +----------------+
     * | HEX('MariaDB') |
     * +----------------+
     * | 4D617269614442 |
     * +----------------+
     * <p>
     * SELECT UNHEX('4D617269614442');
     * +-------------------------+
     * | UNHEX('4D617269614442') |
     * +-------------------------+
     * | MariaDB                 |
     * +-------------------------+
     * <p>
     * SELECT 0x4D617269614442;
     * +------------------+
     * | 0x4D617269614442 |
     * +------------------+
     * | MariaDB          |
     * +------------------+
     * <p>
     * SELECT UNHEX(HEX('string'));
     * +----------------------+
     * | UNHEX(HEX('string')) |
     * +----------------------+
     * | string               |
     * +----------------------+
     * <p>
     * SELECT HEX(UNHEX('1267'));
     * +--------------------+
     * | HEX(UNHEX('1267')) |
     * +--------------------+
     * | 1267               |
     * +--------------------+
     *
     * @param param0Value
     * @return
     */
    public static String UNHEX(Object param0Value) {
        if (param0Value == null) {
            return null;
        }
        if (param0Value instanceof String) {
            byte[] bytes = ((String) param0Value).getBytes();
            String result = HexBin.encode(bytes);
            return result;
        }

        if (param0Value instanceof Number) {
            long value = ((Number) param0Value).longValue();
            String result = Long.toHexString(value).toUpperCase();
            return result;
        }
        throw new IllegalArgumentException();
    }

    /**
     * UPDATEXML
     * Syntax
     * UpdateXML(xml_target, xpath_expr, new_xml)
     * Description
     * This function replaces a single portion of a given fragment of XML markup xml_target with a new XML fragment new_xml, and then returns the changed XML. The portion of xml_target that is replaced matches an XPath expression xpath_expr supplied by the user. If no expression matching xpath_expr is found, or if multiple matches are found, the function returns the original xml_target XML fragment. All three arguments should be strings.
     *
     * Examples
     * SELECT
     *     UpdateXML('<a><b>ccc</b><d></d></a>', '/a', '<e>fff</e>') AS val1,
     *     UpdateXML('<a><b>ccc</b><d></d></a>', '/b', '<e>fff</e>') AS val2,
     *     UpdateXML('<a><b>ccc</b><d></d></a>', '//b', '<e>fff</e>') AS val3,
     *     UpdateXML('<a><b>ccc</b><d></d></a>', '/a/d', '<e>fff</e>') AS val4,
     *     UpdateXML('<a><d></d><b>ccc</b><d></d></a>', '/a/d', '<e>fff</e>') AS val5
     *     \G
     * *************************** 1. row ***************************
     * val1: <e>fff</e>
     * val2: <a><b>ccc</b><d></d></a>
     * val3: <a><e>fff</e><d></d></a>
     * val4: <a><b>ccc</b><e>fff</e></a>
     * val5: <a><d></d><b>ccc</b><d></d></a>
     * 1 row in set (0.00 sec)
     *
     *
     *
     */

    /**
     * WEIGHT_STRING
     * MariaDB starting with 10.0.5
     * The WEIGHT_STRING function was introduced in MariaDB 10.0.5.
     *
     * Syntax
     * WEIGHT_STRING(str [AS {CHAR|BINARY}(N)] [LEVEL levels] [flags])
     *   levels: N [ASC|DESC|REVERSE] [, N [ASC|DESC|REVERSE]] ...
     * Description
     * Returns a binary string representing the string's sorting and comparison value. A string with a lower result means that for sorting purposes the string appears before a string with a higher result.
     *
     * WEIGHT_STRING() is particularly useful when adding new collations, for testing purposes.
     *
     * If str is a non-binary string (CHAR, VARCHAR or TEXT), WEIGHT_STRING returns the string's collation weight. If str is a binary string (BINARY, VARBINARY or BLOB), the return value is simply the input value, since the weight for each byte in a binary string is the byte value.
     *
     * WEIGHT_STRING() returns NULL if given a NULL input.
     *
     * The optional AS clause permits casting the input string to a binary or non-binary string, as well as to a particular length.
     *
     * AS BINARY(N) measures the length in bytes rather than characters, and right pads with 0x00 bytes to the desired length.
     *
     * AS CHAR(N) measures the length in characters, and right pads with spaces to the desired length.
     *
     * N has a minimum value of 1, and if it is less than the length of the input string, the string is truncated without warning.
     *
     * The optional LEVEL clause specifies that the return value should contain weights for specific collation levels. The levels specifier can either be a single integer, a comma-separated list of integers, or a range of integers separated by a dash (whitespace is ignored). Integers can range from 1 to a maximum of 6, dependent on the collation, and need to be listed in ascending order.
     *
     * If the LEVEL clause is no provided, a default of 1 to the maximum for the collation is assumed.
     *
     * If the LEVEL is specified without using a range, an optional modifier is permitted.
     *
     * ASC, the default, returns the weights without any modification.
     *
     * DESC returns bitwise-inverted weights.
     *
     * REVERSE returns the weights in reverse order.
     *
     * Examples
     * The examples below use the HEX() function to represent non-printable results in hexadecimal format.
     *
     * SELECT HEX(WEIGHT_STRING('x'));
     * +-------------------------+
     * | HEX(WEIGHT_STRING('x')) |
     * +-------------------------+
     * | 0058                    |
     * +-------------------------+
     *
     * SELECT HEX(WEIGHT_STRING('x' AS BINARY(4)));
     * +--------------------------------------+
     * | HEX(WEIGHT_STRING('x' AS BINARY(4))) |
     * +--------------------------------------+
     * | 78000000                             |
     * +--------------------------------------+
     *
     * SELECT HEX(WEIGHT_STRING('x' AS CHAR(4)));
     * +------------------------------------+
     * | HEX(WEIGHT_STRING('x' AS CHAR(4))) |
     * +------------------------------------+
     * | 0058002000200020                   |
     * +------------------------------------+
     *
     * SELECT HEX(WEIGHT_STRING(0xaa22ee LEVEL 1));
     * +--------------------------------------+
     * | HEX(WEIGHT_STRING(0xaa22ee LEVEL 1)) |
     * +--------------------------------------+
     * | AA22EE                               |
     * +--------------------------------------+
     *
     * SELECT HEX(WEIGHT_STRING(0xaa22ee LEVEL 1 DESC));
     * +-------------------------------------------+
     * | HEX(WEIGHT_STRING(0xaa22ee LEVEL 1 DESC)) |
     * +-------------------------------------------+
     * | 55DD11                                    |
     * +-------------------------------------------+
     *
     * SELECT HEX(WEIGHT_STRING(0xaa22ee LEVEL 1 REVERSE));
     * +----------------------------------------------+
     * | HEX(WEIGHT_STRING(0xaa22ee LEVEL 1 REVERSE)) |
     * +----------------------------------------------+
     * | EE22AA                                       |
     * +----------------------------------------------+
     *
     */
}