package io.mycat.beans.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.spark.sql.execution.columnar.DOUBLE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@EqualsAndHashCode
@ToString
public class MycatRelDataType {
    final List<MycatField> fieldList;

    public MycatRelDataType(List<MycatField> fieldList) {
        this.fieldList = fieldList;
    }

    public static MycatRelDataType of(List<MycatField> mycatFields) {
        return new MycatRelDataType(mycatFields);
    }

    public MycatRelDataType join(MycatRelDataType right) {
        MycatRelDataType left = this;
        ImmutableList.Builder<MycatField> builder = ImmutableList.builder();
        builder.addAll(left.fieldList);
        builder.addAll(right.fieldList);
        return of(builder.build());
    }

    public MycatRelDataType rename(List<String> names) {
        if (fieldList.size() != names.size()) {
            throw new IllegalArgumentException();
        }
        ImmutableList.Builder<MycatField> builder = ImmutableList.builder();
        for (int i = 0; i < fieldList.size(); i++) {
            MycatField mycatField = fieldList.get(i);
            String newName = names.get(i);
            if (newName.equals(mycatField.getName())) {
                builder.add(mycatField);
            } else {
                builder.add(mycatField.rename(newName));
            }
        }
        return MycatRelDataType.of(builder.build());
    }


    public static MycatRelDataType getMycatRelType(SQLCreateTableStatement statement) {
        List<SQLColumnDefinition> columnDefinitions = statement.getColumnDefinitions();
        List<MycatField> mycatFields = new ArrayList<>(columnDefinitions.size());
        for (SQLColumnDefinition columnDefinition : columnDefinitions) {
            columnDefinition.simplify();
            String columnName = SQLUtils.normalize(columnDefinition.getColumnName());
            SQLDataType dataType = columnDefinition.getDataType();
            String dataTypeString = dataType.toString().toUpperCase();
            String name = SQLUtils.normalize(dataType.getName()).toUpperCase();
            List<SQLExpr> arguments = dataType.getArguments();
            Class klass;
            String mysqlTypeName;

            boolean unsigned = dataTypeString.contains("UNSIGNED");
            boolean binary = dataTypeString.contains("BLOB")||dataTypeString.contains("BINARY") || dataType.isString() && dataType instanceof SQLCharacterDataType && ((SQLCharacterDataType) dataType).isHasBinary();
            boolean notNull = columnDefinition.containsNotNullConstaint();
            int mysqlFlag;
            int scale = 0;//M
            int precision = 0;
            MycatDataType mycatType = null;
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
                    if (unsigned) {
                        mysqlTypeName = "TINYINT";
                        klass = Short.class;
                        mycatType = MycatDataType.UNSIGNED_TINYINT;
                    } else {
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
                        mycatType = MycatDataType.SHORT;
                    } else {
                        klass = Integer.class;
                        mysqlTypeName = "SMALLINT UNSIGNED";
                        mycatType = MycatDataType.UNSIGNED_SHORT;
                    }
                    break;
                }
                case "MEDIUMINT": {
                    klass = Integer.class;
                    if (!unsigned) {
                        mysqlTypeName = "MEDIUMINT";
                        mycatType = MycatDataType.INT;
                    } else {
                        mysqlTypeName = "MEDIUMINT UNSIGNED";
                        mycatType = MycatDataType.UNSIGNED_INT;
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
                    if (arguments.size() > 0) {
                        scale = Integer.parseInt(arguments.get(0).toString());
                    }
                    if (arguments.size() > 1) {
                        precision = Integer.parseInt(arguments.get(1).toString());
                    }
                    mycatType = MycatDataType.FLOAT;
                    break;
                }
                case "DOUBLE": {
                    mysqlTypeName = "DOUBLE";
                    klass = DOUBLE.class;
                    if (arguments.size() > 0) {
                        scale = Integer.parseInt(arguments.get(0).toString());
                    }
                    mycatType = MycatDataType.DOUBLE;
                    break;
                }
                case "DECIMAL": {
                    mysqlTypeName = "DECIMAL";
                    klass = BigDecimal.class;
                    if (arguments.size() > 0) {
                        scale = Integer.parseInt(arguments.get(0).toString());
                    }
                    if (arguments.size() > 1) {
                        precision = Integer.parseInt(arguments.get(1).toString());
                    }
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
                        mycatType = MycatDataType.BINARY;
                    } else {
                        klass = String.class;
                        mycatType = MycatDataType.CHAR;
                    }
                    break;
                }
                default:
                case "VARCHAR": {
                    mysqlTypeName = "VARCHAR";
                    if (binary) {
                        klass = byte[].class;
                        mycatType = MycatDataType.BINARY;
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
            MycatField mycatField = MycatField.of(columnName, mycatType, !notNull, scale, precision);
            mycatFields.add(mycatField);
        }
        MycatRelDataType mycatRelDataType = MycatRelDataType.of(mycatFields);
        return mycatRelDataType;
    }
}
