package io.mycat.sqlEngine.ast;

import java.sql.Types;

/**
 * Converts database types to Java class types.
 */
public class SQLTypeMap {
    /**
     * Translates a data type from an integer (java.sqlEngine.Types value) to a string
     * that represents the corresponding class.
     * 
     * @param type
     *            The java.sqlEngine.Types value to convert to its corresponding class.
     * @return The class that corresponds to the given java.sqlEngine.Types
     *         value, or Object.class if the type has no known mapping.
     */
    public static Class<?> toClass(int type) {
        Class<?> result = Object.class;

        switch (type) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                result = String.class;
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                result = java.math.BigDecimal.class;
                break;

            case Types.BIT:
                result = Long.class;
                break;

            case Types.TINYINT:
                result = Long.class;
                break;

            case Types.SMALLINT:
                result = Long.class;
                break;

            case Types.INTEGER:
                result = Long.class;
                break;

            case Types.BIGINT:
                result = Long.class;
                break;

            case Types.REAL:
            case Types.FLOAT:
                result = Double.class;
                break;

            case Types.DOUBLE:
                result =  Double.class;
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                result = Byte[].class;
                break;

            case Types.DATE:
                result = java.sql.Date.class;
                break;

            case Types.TIME:
                result = java.sql.Time.class;
                break;

            case Types.TIMESTAMP:
                result = java.sql.Timestamp.class;
                break;
        }

        return result;
    }
}