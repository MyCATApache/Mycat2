package io.mycat.beans.mysql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import io.mycat.beans.mycat.MycatDataType;
import org.apache.spark.sql.execution.columnar.DOUBLE;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class MySQLTypeParser {

    public static void main(String[] args) {
        SQLCreateTableStatement statement = (SQLCreateTableStatement) SQLUtils.parseSingleStatement("CREATE TABLE `setcharsetdemo` (`FirstName` varchar(60) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci", DbType.mysql);
        List<SQLColumnDefinition> columnDefinitions = statement.getColumnDefinitions();
        for (SQLColumnDefinition columnDefinition : columnDefinitions) {
            columnDefinition.simplify();
            SQLDataType dataType = columnDefinition.getDataType();
            String name = SQLUtils.normalize(dataType.getName()).toUpperCase();
            List<SQLExpr> arguments = dataType.getArguments();
            Class klass;
            String mysqlTypeName;
            boolean unsigned = dataType.isInt() && dataType.toString().toUpperCase().contains("UNSIGNED");
            boolean binary = dataType.isString() && dataType instanceof SQLCharacterDataType && ((SQLCharacterDataType) dataType).isHasBinary();
            boolean notNull = columnDefinition.containsNotNullConstaint();
            int mysqlFlag;
            MycatDataType mycatType;
            switch (name) {
                case "BIT": {
                    if (arguments.size() == 1 && "1".equals(arguments.get(0).toString())) {
                        mysqlTypeName = "BIT(1)";
                        klass = Boolean.class;
                        mycatType = MycatDataType.BOOLEAN;
                    } else {
                        mysqlTypeName = "BIT";
                        klass = Long.class;
                        mycatType = MycatDataType.BIT;
                    }
                    break;
                }
                case "TINYINT": {
                    if (arguments.size() == 1 && "1".equals(arguments.get(0).toString())) {
                        mysqlTypeName = "TINYINT(1)";
                        klass = Boolean.class;
                        mycatType = MycatDataType.BOOLEAN;
                    } else if (unsigned){
                        mysqlTypeName = "TINYINT";
                        klass = Short.class;
                        mycatType = MycatDataType.UNSIGNED_TINYINT;
                    }else{
                        mysqlTypeName = "TINYINT";
                        klass = Byte.class;
                        mycatType = MycatDataType.TINYINT;
                    }
                    break;
                }
                case "BOOLEAN":
                case "BOOL": {
                    mysqlTypeName = "TINYINT(1)";
                    klass = Boolean.class;
                    mycatType = MycatDataType.BOOLEAN;
                    break;
                }
                case "SMALLINT": {
                    if (!unsigned) {
                        klass = Short.class;
                        mysqlTypeName = "SMALLINT";
                        mycatType = MycatDataType.TINYINT;
                    } else {
                        klass = Integer.class;
                        mysqlTypeName = "SMALLINT UNSIGNED";
                        mycatType = MycatDataType.UNSIGNED_TINYINT;
                    }
                    break;
                }
                case "MEDIUMINT": {
                    klass = Integer.class;
                    if (!unsigned) {
                        mysqlTypeName = "MEDIUMINT";
                        mycatType = MycatDataType.SHORT;
                    } else {
                        mysqlTypeName = "MEDIUMINT UNSIGNED";
                        mycatType = MycatDataType.UNSIGNED_SHORT;
                    }
                    break;
                }
                case "INT":
                case "INTEGER": {
                    if (!unsigned) {
                        mysqlTypeName = "INT";
                        klass = Integer.class;
                        mycatType = MycatDataType.INT;
                    } else {
                        mysqlTypeName = "INT UNSIGNED";
                        klass = Long.class;
                        mycatType = MycatDataType.UNSIGNED_INT;
                    }
                    break;
                }
                case "BIGINT": {
                    if (!unsigned) {
                        mysqlTypeName = "BIGINT";
                        klass = Long.class;
                        mycatType = MycatDataType.LONG;
                    } else {
                        mysqlTypeName = "BIGINT UNSIGNED";
                        klass = BigInteger.class;
                        mycatType = MycatDataType.UNSIGNED_LONG;
                    }
                    break;
                }
                case "FLOAT": {
                    mysqlTypeName = "FLOAT";
                    klass = Float.class;
                    mycatType = MycatDataType.FLOAT;
                    break;
                }
                case "DOUBLE": {
                    mysqlTypeName = "DOUBLE";
                    klass = DOUBLE.class;
                    mycatType = MycatDataType.DOUBLE;
                    break;
                }
                case "DECIMAL": {
                    mysqlTypeName = "DECIMAL";
                    klass = java.math.BigDecimal.class;
                    mycatType = MycatDataType.DECIMAL;
                    break;
                }
                case "DATE": {
                    mysqlTypeName = "DATE";
                    klass = LocalDate.class;
                    mycatType = MycatDataType.DATE;
                    break;
                }
                case "DATETIME":
                case "TIMESTAMP": {
                    mysqlTypeName = "DATETIME";
                    klass = LocalDateTime.class;
                    mycatType = MycatDataType.DATETIME;
                    break;
                }
                case "TIME": {
                    mysqlTypeName = "TIME";
                    klass = Duration.class;
                    mycatType = MycatDataType.TIME;
                    break;
                }
                case "YEAR": {
                    mysqlTypeName = "YEAR";
                    klass = Short.class;
                    mycatType = MycatDataType.YEAR;
                    break;
                }
                case "CHAR": {
                    mysqlTypeName = "CHAR";
                    if (binary) {
                        klass = byte[].class;
                        mycatType = MycatDataType.CHAR_BINARY;
                    } else {
                        klass = String.class;
                        mycatType = MycatDataType.CHAR;
                    }
                    break;
                }
                case "VARCHAR": {
                    mysqlTypeName = "VARCHAR";
                    if (binary) {
                        klass = byte[].class;
                        mycatType = MycatDataType.VARCHAR_BINARY;
                    } else {
                        klass = String.class;
                        mycatType = MycatDataType.VARCHAR;
                    }
                    break;
                }
                case "BINARY": {
                    mysqlTypeName = "BINARY";
                    klass = byte[].class;
                    mycatType = MycatDataType.BINARY;
                    break;
                }
                case "VARBINARY": {
                    mysqlTypeName = "VARBINARY";
                    klass = byte[].class;
                    mycatType = MycatDataType.BINARY;
                    break;
                }
                case "TINYTEXT": {
                    mysqlTypeName = "TINYTEXT";
                    klass = String.class;
                    mycatType = MycatDataType.VARCHAR;
                    break;
                }
                case "BLOB": {
                    mysqlTypeName = "BLOB";
                    klass = byte[].class;
                    mycatType = MycatDataType.BINARY;
                    break;
                }
                case "TEXT":
                case "LONGTEXT": {
                    mysqlTypeName = "VARCHAR";
                    klass = String.class;
                    mycatType = MycatDataType.VARCHAR;
                    break;
                }
                case "MEDIUMBLOB": {
                    mysqlTypeName = "MEDIUMBLOB";
                    klass = byte[].class;
                    mycatType = MycatDataType.BINARY;
                    break;
                }
                case "MEDIUMTEXT": {
                    mysqlTypeName = "MEDIUMTEXT";
                    klass = String.class;
                    mycatType = MycatDataType.VARCHAR;
                    break;
                }
                case "LONGBLOB": {
                    mysqlTypeName = "LONGBLOB";
                    klass = byte[].class;
                    mycatType = MycatDataType.BINARY;
                    break;
                }
                case "SET":
                case "ENUM": {
                    mysqlTypeName = "CHAR";
                    klass = String.class;
                    mycatType = MycatDataType.CHAR;
                    break;
                }
            }

            System.out.println();
        }

        System.out.println();

    }
}
