package io.mycat.calcite;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
class SimpleColumnInfo {
    String columnName;
    int dataType;
    int precision;
    int scale;
    String typeString;
    boolean nullable;
}